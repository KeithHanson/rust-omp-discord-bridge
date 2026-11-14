use super::*;
use serde_json::{Value, json};
use serenity::all::{
    ActionRowComponent, ComponentInteractionDataKind, CreateActionRow, CreateAttachment,
    CreateInputText, CreateModal, CreateSelectMenu, CreateSelectMenuKind, CreateSelectMenuOption,
    InputTextStyle,
};

const PREFIX: &str = "omp-prompt:";
const CARD: &str = "**OMP question**";
const PAGE_SIZE: usize = 25;
const EXPIRED: &str = "This OMP question expired, changed, or was already answered. Use the latest question.";

enum PromptKind {
    Select { options: Vec<String>, descriptions: Vec<String>, checked: Vec<usize>, markable: usize },
    Text { draft: String, placeholder: String },
}

struct PromptSpec {
    title: String,
    kind: PromptKind,
    timeout: Duration,
}

impl PromptSpec {
    fn parse(request: &Value) -> Result<Self> {
        if request.get("secret").and_then(Value::as_bool) == Some(true) {
            bail!("Secret input is not supported in Discord. No plaintext input will be requested.");
        }
        let kind = match request["method"].as_str() {
            Some("select") => {
                let options = request["options"].as_array().context("Select options must be an array")?
                    .iter().map(|v| v.as_str().map(str::to_owned).context("Select options must be strings"))
                    .collect::<Result<Vec<_>>>()?;
                if options.is_empty() { bail!("Select options cannot be empty"); }
                let descriptions = (0..options.len()).map(|index| {
                    request["optionDetails"][index]["description"].as_str().unwrap_or("").to_owned()
                }).collect();
                let checked = request["checkedIndices"].as_array().map(|indices| {
                    indices.iter().filter_map(Value::as_u64).filter_map(|i| usize::try_from(i).ok())
                        .filter(|i| *i < options.len()).collect()
                }).unwrap_or_default();
                let markable = request["markableCount"].as_u64().and_then(|n| usize::try_from(n).ok())
                    .unwrap_or(options.len()).min(options.len());
                PromptKind::Select { options, descriptions, checked, markable }
            }
            Some("input" | "editor") => PromptKind::Text {
                draft: request["prefill"].as_str().unwrap_or("").to_owned(),
                placeholder: request["placeholder"].as_str().unwrap_or("").to_owned(),
            },
            _ => bail!("This OMP question type is not supported in Discord; use select, input, or editor fields."),
        };
        Ok(Self {
            title: request["title"].as_str().unwrap_or("OMP needs your answer").to_owned(),
            kind,
            timeout: request["timeout"].as_u64().map(Duration::from_millis).unwrap_or(APPROVAL_TIMEOUT),
        })
    }

    fn context(&self) -> String {
        let mut text = self.title.clone();
        match &self.kind {
            PromptKind::Select { options, descriptions, checked, markable } => {
                for (index, option) in options.iter().enumerate() {
                    let marker = if checked.contains(&index) && index < *markable { "[x] " } else { "" };
                    text.push_str(&format!("\n\n{}. {marker}{option}", index + 1));
                    if !descriptions[index].is_empty() {
                        text.push('\n');
                        text.push_str(&descriptions[index]);
                    }
                }
            }
            PromptKind::Text { draft, placeholder } => {
                if !placeholder.is_empty() { text.push_str(&format!("\n\nHint: {placeholder}")); }
                if !draft.is_empty() { text.push_str(&format!("\n\nInitial text (preserved exactly):\n{draft}")); }
            }
        }
        text
    }
}

#[derive(Default)]
pub(super) struct PromptBroker {
    next_id: AtomicU64,
    pending: Mutex<HashMap<String, PendingPrompt>>,
}

struct PendingPrompt {
    thread_id: ThreadId,
    message_id: Option<MessageId>,
    spec: PromptSpec,
    page: usize,
    revision: u64,
    modal: Option<(u64, u64, bool)>, // interaction nonce, user, append
    sender: oneshot::Sender<Option<Value>>,
}

struct PromptRegistration<'a> {
    broker: &'a PromptBroker,
    id: String,
    thread_id: ThreadId,
    message_id: Option<MessageId>,
    receiver: oneshot::Receiver<Option<Value>>,
    http: Option<Arc<Http>>,
    store: Option<Store>,
    finished: bool,
}

impl Drop for PromptRegistration<'_> {
    fn drop(&mut self) {
        self.broker.pending.lock().expect("prompt broker poisoned").remove(&self.id);
        if !self.finished
            && let (Some(http), Some(store), Some(message), Ok(runtime)) =
                (self.http.clone(), self.store.clone(), self.message_id, tokio::runtime::Handle::try_current())
        {
            let thread = self.thread_id;
            runtime.spawn(async move {
                if let Ok(_access) = thread_access(&store, thread).await {
                    close_card(&http, thread, message, "Cancelled; this question is no longer active.").await;
                }
            });
        }
    }
}

impl PromptRegistration<'_> {
    fn bind(&mut self, message: MessageId) {
        self.message_id = Some(message);
        if let Some(pending) = self.broker.pending.lock().expect("prompt broker poisoned").get_mut(&self.id) {
            pending.message_id = Some(message);
        }
    }
}

fn short(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit { text.to_owned() }
    else { text.chars().take(limit - 3).chain("...".chars()).collect() }
}

impl PendingPrompt {
    fn custom_id(&self, id: &str, action: &str) -> String {
        format!("{PREFIX}{id}:{}:{action}", self.revision)
    }

    fn card(&self, id: &str) -> (String, Vec<CreateActionRow>) {
        let button = |action, label| CreateButton::new(self.custom_id(id, action)).label(label);
        let mut rows = Vec::new();
        let detail = match &self.spec.kind {
            PromptKind::Select { options, descriptions, checked, markable } => {
                let start = self.page * PAGE_SIZE;
                let menu_options = options.iter().enumerate().skip(start).take(PAGE_SIZE).map(|(index, option)| {
                    let marker = if checked.contains(&index) && index < *markable { "[x] " } else { "" };
                    let label = short(&format!("{}. {marker}{option}", index + 1), 100);
                    let mut item = CreateSelectMenuOption::new(label, index.to_string());
                    if !descriptions[index].is_empty() { item = item.description(short(&descriptions[index], 100)); }
                    item
                }).collect();
                rows.push(CreateActionRow::SelectMenu(CreateSelectMenu::new(
                    self.custom_id(id, "select"), CreateSelectMenuKind::String { options: menu_options },
                ).min_values(1).max_values(1).placeholder("Choose one option / toggle one checkbox")));
                let pages = options.len().div_ceil(PAGE_SIZE);
                rows.push(CreateActionRow::Buttons(vec![
                    button("previous", "Previous page").disabled(self.page == 0),
                    button("next", "Next page").disabled(self.page + 1 == pages),
                    button("cancel", "Cancel").style(ButtonStyle::Danger),
                ]));
                format!("Page {} of {pages}. Pick one option. Checked items show [x]; checkbox forms return with updated choices until you choose their Done option.", self.page + 1)
            }
            PromptKind::Text { draft, .. } => {
                rows.push(CreateActionRow::Buttons(vec![
                    button("replace", "Edit / replace text"), button("append", "Append text"),
                    button("submit", "Submit answer").style(ButtonStyle::Success),
                    button("cancel", "Cancel").style(ButtonStyle::Danger),
                ]));
                format!("Draft: {} characters. Edit in a modal, then Submit answer. For more than 4,000 characters, Append text in multiple parts (joined exactly, with no added newline). Replacing a draft over 4,000 characters starts blank and replaces ALL text. Answers are not secret.", draft.chars().count())
            }
        };
        (format!("{CARD}\n{}\n\n{detail}\n\nFull question and unabridged choices/text are above. Use these controls, not an ordinary message. Expires after {} seconds.", short(&self.spec.title, 650), self.spec.timeout.as_secs()), rows)
    }
}

struct PromptActor {
    thread: ThreadId,
    message: Option<MessageId>,
    user: u64,
    authorized: bool,
}

impl PromptBroker {
    fn register(&self, thread_id: ThreadId, spec: PromptSpec) -> PromptRegistration<'_> {
        let epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
        let id = format!("{epoch:x}-{:x}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().expect("prompt broker poisoned").insert(id.clone(), PendingPrompt {
            thread_id, message_id: None, spec, page: 0, revision: 0, modal: None, sender,
        });
        PromptRegistration { broker: self, id, thread_id, message_id: None, receiver, http: None, store: None, finished: false }
    }

    fn interact(
        &self, custom_id: &str, actor: PromptActor,
        nonce: u64, values: &[String], text: Option<&str>,
    ) -> std::result::Result<CreateInteractionResponse, &'static str> {
        let PromptActor { thread, message, user, authorized } = actor;
        if !authorized { return Err("You are not authorized to answer this OMP question."); }
        let mut parts = custom_id.strip_prefix(PREFIX).ok_or(EXPIRED)?.split(':');
        let id = parts.next().ok_or(EXPIRED)?;
        let revision = parts.next().and_then(|s| s.parse::<u64>().ok()).ok_or(EXPIRED)?;
        let action = parts.next().ok_or(EXPIRED)?;
        if parts.next().is_some() { return Err(EXPIRED); }
        let mut pending = self.pending.lock().expect("prompt broker poisoned");
        let request = pending.get_mut(id).ok_or(EXPIRED)?;
        if request.thread_id != thread || message.is_none() || request.message_id != message {
            return Err(EXPIRED);
        }
        if request.sender.is_closed() { return Err(EXPIRED); }
        let mut terminal = None;
        if action == "modal" {
            let Some((ticket, owner, append)) = request.modal else { return Err(EXPIRED); };
            if ticket != revision || owner != user { return Err(EXPIRED); }
            let text = text.ok_or("Missing text field.")?;
            if text.chars().count() > 4000 { return Err("Each text part must be at most 4,000 characters."); }
            let PromptKind::Text { draft, .. } = &mut request.spec.kind else { return Err(EXPIRED); };
            if append { draft.push_str(text); } else { *draft = text.to_owned(); }
            request.modal = None;
        } else {
            if revision != request.revision || text.is_some() { return Err(EXPIRED); }
            match action {
                "cancel" => terminal = Some(None),
                "select" => {
                    let PromptKind::Select { options, .. } = &request.spec.kind else { return Err(EXPIRED); };
                    if values.len() != 1 { return Err("Choose exactly one option."); }
                    let index = values[0].parse::<usize>().map_err(|_| "Invalid choice.")?;
                    if index / PAGE_SIZE != request.page { return Err(EXPIRED); }
                    terminal = Some(Some(Value::String(options.get(index).ok_or("Invalid choice.")?.clone())));
                }
                "previous" | "next" => {
                    let PromptKind::Select { options, .. } = &request.spec.kind else { return Err(EXPIRED); };
                    let page = if action == "previous" { request.page.checked_sub(1) } else { request.page.checked_add(1) }.ok_or(EXPIRED)?;
                    if page >= options.len().div_ceil(PAGE_SIZE) { return Err(EXPIRED); }
                    request.page = page;
                }
                "replace" | "append" => {
                    let PromptKind::Text { draft, placeholder } = &request.spec.kind else { return Err(EXPIRED); };
                    let append = action == "append";
                    let mut field = CreateInputText::new(InputTextStyle::Paragraph, if append { "Append exactly (include needed newlines)" } else { "Replace entire answer" }, "answer")
                        .required(false).max_length(4000);
                    if !append && draft.chars().count() <= 4000 { field = field.value(draft); }
                    if !placeholder.is_empty() { field = field.placeholder(short(placeholder, 100)); }
                    request.modal = Some((nonce, user, append));
                    return Ok(CreateInteractionResponse::Modal(CreateModal::new(
                        format!("{PREFIX}{id}:{nonce}:modal"), "OMP answer (not secret)",
                    ).components(vec![CreateActionRow::InputText(field)])));
                }
                "submit" => {
                    let PromptKind::Text { draft, .. } = &request.spec.kind else { return Err(EXPIRED); };
                    terminal = Some(Some(Value::String(draft.clone())));
                }
                _ => return Err(EXPIRED),
            }
            request.modal = None;
        }
        if let Some(value) = terminal {
            let cancelled = value.is_none();
            let request = pending.remove(id).ok_or(EXPIRED)?;
            request.sender.send(value).map_err(|_| EXPIRED)?;
            // The owning future closes the card. An ephemeral acknowledgement does not
            // race that edit with an older card snapshot.
            return Ok(feedback(if cancelled { "OMP question cancelled." } else { "Answer sent to OMP." }));
        }
        request.revision += 1;
        let (content, components) = request.card(id);
        Ok(CreateInteractionResponse::UpdateMessage(CreateInteractionResponseMessage::new()
            .content(content).components(components).allowed_mentions(CreateAllowedMentions::new())))
    }
}

fn feedback(content: &str) -> CreateInteractionResponse {
    CreateInteractionResponse::Message(CreateInteractionResponseMessage::new()
        .content(content).ephemeral(true).allowed_mentions(CreateAllowedMentions::new()))
}

async fn close_card(http: &Http, thread: ThreadId, message: MessageId, status: &str) {
    if let Err(error) = SerenityChannelId::new(thread.get()).edit_message(
        http, serenity::all::MessageId::new(message.get()), EditMessage::new()
            .content(format!("{CARD}\n{status}")).components(Vec::new())
            .allowed_mentions(CreateAllowedMentions::new()),
    ).await {
        tracing::warn!(%thread, %message, %error, "failed to close OMP question card");
    }
}

impl DiscordMessenger {
    pub(super) async fn prompt_inner(&self, thread_id: ThreadId, request: Value) -> Value {
        let mut response = json!({"type":"extension_ui_response", "id":request["id"], "cancelled":true});
        let result: Result<Option<Value>> = async {
            let access = thread_access(&self.store, thread_id).await?;
            let spec = PromptSpec::parse(&request)?;
            let timeout = spec.timeout;
            let context = spec.context();
            let mut registration = self.prompts.register(thread_id, spec);
            registration.http = Some(Arc::clone(&self.http));
            registration.store = Some(self.store.clone());
            // A file preserves every byte when Discord's message or menu labels would
            // truncate the title, descriptions, choices, or editor prefill.
            let mut context_message = CreateMessage::new().allowed_mentions(CreateAllowedMentions::new());
            if context.chars().count() <= 1700 {
                context_message = context_message.content(format!("{CARD} context:\n{}", operational_content(&context)));
            } else {
                context_message = context_message.content(format!("{CARD} context: full text attached (including every option and description)."))
                    .add_file(CreateAttachment::bytes(context.into_bytes(), "omp-question.txt"));
            }
            let channel = SerenityChannelId::new(thread_id.get());
            channel.send_message(&self.http, context_message).await?;
            let (content, components) = self.prompts.pending.lock().expect("prompt broker poisoned")
                .get(&registration.id).context("Question registration disappeared")?.card(&registration.id);
            let message = channel.send_message(&self.http, CreateMessage::new()
                .content(content).components(components).allowed_mentions(CreateAllowedMentions::new())).await?;
            registration.bind(MessageId::new(message.id.get()));
            drop(access);
            let (answer, status) = match tokio::time::timeout(timeout, &mut registration.receiver).await {
                Ok(Ok(Some(answer))) => (Some(answer), "Answered; sent to OMP."),
                Ok(_) => (None, "Cancelled; no answer sent."),
                Err(_) => {
                    response["timedOut"] = Value::Bool(true);
                    (None, "Timed out; no answer sent.")
                }
            };
            // Remove authorization before awaiting Discord cleanup.
            self.prompts.pending.lock().expect("prompt broker poisoned").remove(&registration.id);
            if let Ok(_access) = thread_access(&self.store, thread_id).await {
                close_card(&self.http, thread_id, MessageId::new(message.id.get()), status).await;
            }
            registration.finished = true;
            Ok(answer)
        }.await;
        match result {
            Ok(Some(value)) => {
                response.as_object_mut().expect("response object").remove("cancelled");
                response["value"] = value;
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(%thread_id, %error, "failed to present OMP question");
                let _ = self.send(thread_id, &format!("OMP question cancelled: {error}")).await;
            }
        }
        response
    }
}

impl DiscordHandler {
    pub(super) async fn prompt_interaction(&self, http: &Http, interaction: &Interaction) -> bool {
        let (custom_id, channel, message, guild, user, nonce, values, text) = match interaction {
            Interaction::Component(component) if component.data.custom_id.starts_with(PREFIX) => {
                let values = match &component.data.kind {
                    ComponentInteractionDataKind::StringSelect { values } => values.as_slice(),
                    _ => &[],
                };
                (component.data.custom_id.as_str(), component.channel_id, Some(MessageId::new(component.message.id.get())),
                 component.guild_id, &component.user, component.id.get(), values, None)
            }
            Interaction::Modal(modal) if modal.data.custom_id.starts_with(PREFIX) => {
                let text = modal.data.components.iter().flat_map(|row| &row.components).find_map(|component| match component {
                    ActionRowComponent::InputText(input) if input.custom_id == "answer" => input.value.as_deref(),
                    _ => None,
                });
                (modal.data.custom_id.as_str(), modal.channel_id, modal.message.as_ref().map(|m| MessageId::new(m.id.get())),
                 modal.guild_id, &modal.user, modal.id.get(), &[][..], text)
            }
            _ => return false,
        };
        let thread = ThreadId::new(channel.get());
        let authorized = match self.store.session(thread).await {
            Ok(Some(session)) if guild.map(|id| id.get()) == Some(session.guild_id.get()) => self.config.guild_policy(session.guild_id)
                .is_some_and(|policy| permits_approval(policy, session.parent_channel_id, user)),
            _ => false,
        };
        let response = self.prompts.interact(custom_id, PromptActor { thread, message, user: user.id.get(), authorized }, nonce, values, text)
            .unwrap_or_else(feedback);
        let result = match interaction {
            Interaction::Component(component) => component.create_response(http, response).await,
            Interaction::Modal(modal) => modal.create_response(http, response).await,
            _ => unreachable!(),
        };
        if let Err(error) = result { tracing::warn!(%error, "failed to acknowledge OMP question interaction"); }
        true
    }

    pub(super) async fn prompt_reply(&self, http: &Http, message: &Message, bot_id: u64) -> Result<bool> {
        let Some(replied) = message.referenced_message.as_ref() else { return Ok(false); };
        if replied.author.id.get() != bot_id || !replied.content.starts_with(CARD) { return Ok(false); }
        // Replies to active AND stale question cards never become new turns, even
        // if the original broker entry has been cancelled or the daemon restarted.
        send_without_mentions(http, ThreadId::new(message.channel_id.get()),
            "Use the latest OMP question's buttons and text modal to answer. This reply was not submitted as a new turn; closed questions cannot be answered.").await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn select() -> PromptSpec {
        PromptSpec::parse(&json!({"method":"select", "title":"Choose", "options":["First", "Long exact second"],
            "optionDetails":[{}, {"description":"Full description"}], "checkedIndices":[1], "markableCount":2})).unwrap()
    }

    fn action(broker: &PromptBroker, id: &str, action: &str, authorized: bool, values: &[String]) -> std::result::Result<CreateInteractionResponse, &'static str> {
        broker.interact(&format!("{PREFIX}{id}:0:{action}"), actor(1, 2, 3, authorized), 4, values, None)
    }

    fn actor(thread: u64, message: u64, user: u64, authorized: bool) -> PromptActor {
        PromptActor { thread: ThreadId::new(thread), message: Some(MessageId::new(message)), user, authorized }
    }

    #[test]
    fn validates_schema_and_refuses_secrets() {
        for request in [json!({"method":"input","secret":true}), json!({"method":"select","options":[]}),
            json!({"method":"select","options":[1]}), json!({"method":"ask"})] {
            assert!(PromptSpec::parse(&request).is_err());
        }
        let spec = select();
        assert!(spec.context().contains("2. [x] Long exact second\nFull description"));
        assert_eq!(PromptSpec::parse(&json!({"method":"input","timeout":123})).unwrap().timeout, Duration::from_millis(123));
    }

    #[tokio::test]
    async fn exact_selection_authorization_scope_and_replay() {
        let broker = PromptBroker::default();
        let mut registration = broker.register(ThreadId::new(1), select());
        registration.bind(MessageId::new(2));
        let values = vec!["1".to_owned()];
        assert!(action(&broker, &registration.id, "select", false, &values).is_err());
        for (thread, message) in [(9, 2), (1, 9)] {
            assert!(broker.interact(&format!("{PREFIX}{}:0:select", registration.id), actor(thread, message, 3, true), 4, &values, None).is_err());
        }
        assert!(action(&broker, &registration.id, "select", true, &["99".to_owned()]).is_err());
        assert!(action(&broker, &registration.id, "select", true, &values).is_ok());
        assert_eq!((&mut registration.receiver).await.unwrap(), Some(json!("Long exact second")));
        assert!(action(&broker, &registration.id, "select", true, &values).is_err());
    }

    #[tokio::test]
    async fn cancellation_and_drop_revoke_controls() {
        let broker = PromptBroker::default();
        let mut registration = broker.register(ThreadId::new(1), select());
        registration.bind(MessageId::new(2));
        assert!(action(&broker, &registration.id, "cancel", true, &[]).is_ok());
        assert_eq!((&mut registration.receiver).await.unwrap(), None);
        assert!(action(&broker, &registration.id, "cancel", true, &[]).is_err());
        let registration = broker.register(ThreadId::new(1), select());
        let id = registration.id.clone();
        drop(registration);
        assert!(!broker.pending.lock().unwrap().contains_key(&id));
    }

    #[tokio::test]
    async fn modal_is_user_bound_single_use_and_appends_without_truncation() {
        let broker = PromptBroker::default();
        let initial = "a".repeat(5000);
        let spec = PromptSpec::parse(&json!({"method":"editor","prefill":initial})).unwrap();
        let mut registration = broker.register(ThreadId::new(1), spec);
        registration.bind(MessageId::new(2));
        assert!(action(&broker, &registration.id, "append", true, &[]).is_ok());
        let modal = format!("{PREFIX}{}:4:modal", registration.id);
        assert!(broker.interact(&modal, actor(1, 2, 9, true), 5, &[], Some("\nlast")).is_err());
        assert!(broker.interact(&modal, actor(1, 2, 3, true), 5, &[], Some("\nlast")).is_ok());
        assert!(broker.interact(&modal, actor(1, 2, 3, true), 6, &[], Some("duplicate")).is_err());
        assert!(broker.interact(&format!("{PREFIX}{}:1:submit", registration.id), actor(1, 2, 3, true), 7, &[], None).is_ok());
        assert_eq!((&mut registration.receiver).await.unwrap(), Some(json!(format!("{initial}\nlast"))));
    }

    #[test]
    fn pagination_rejects_off_page_choices_and_stale_controls() {
        let broker = PromptBroker::default();
        let options = (0..30).map(|i| format!("Option {i}")).collect::<Vec<_>>();
        let spec = PromptSpec::parse(&json!({"method":"select","options":options})).unwrap();
        let mut registration = broker.register(ThreadId::new(1), spec);
        registration.bind(MessageId::new(2));
        assert!(action(&broker, &registration.id, "select", true, &["25".into()]).is_err());
        assert!(action(&broker, &registration.id, "next", true, &[]).is_ok());
        assert!(action(&broker, &registration.id, "select", true, &["0".into()]).is_err());
        assert!(broker.interact(&format!("{PREFIX}{}:1:select", registration.id), actor(1, 2, 3, true), 7, &["25".into()], None).is_ok());
    }
}
