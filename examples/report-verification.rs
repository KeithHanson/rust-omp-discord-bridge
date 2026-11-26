use std::{
    collections::VecDeque,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use omp_client_bridge::{
    config::AppConfig,
    domain::{
        ChannelId, ConversationId, GuildId, MessageId, Session, SessionStatus, ThreadId, Turn,
        TurnStatus, UserId,
    },
    omp::{ApprovalRequest, OmpTransport, OmpTurnError, OmpUi, TurnControl, TurnInput},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Control {
    queue: Mutex<VecDeque<Turn>>,
    results: Mutex<Vec<(i64, TurnStatus)>>,
}

#[async_trait]
impl TurnControl for Control {
    async fn next_turn(&self) -> Result<Option<Turn>, OmpTurnError> {
        Ok(self.queue.lock().unwrap().pop_front())
    }
    async fn record_result(
        &self,
        id: i64,
        status: TurnStatus,
        _: &str,
        _: Option<&str>,
    ) -> Result<(), OmpTurnError> {
        self.results.lock().unwrap().push((id, status));
        Ok(())
    }
    async fn save_session(&self, _: &Path) -> Result<(), OmpTurnError> {
        Ok(())
    }
}

struct Ui {
    command_started: Notify,
    approval_started: Notify,
}

#[async_trait]
impl OmpUi for Ui {
    fn message(&self, text: &str) -> Result<(), OmpTurnError> {
        eprintln!("{text}");
        Ok(())
    }
    fn activity(&self, label: &'static str) {
        if label == "Running a command" {
            self.command_started.notify_one();
        }
    }
    async fn confirm(&self, _: ApprovalRequest) -> bool {
        self.approval_started.notify_one();
        std::future::pending().await
    }
    async fn prompt(&self, request: serde_json::Value) -> serde_json::Value {
        panic!("Unexpected interactive prompt in report verification: {request}");
    }
    async fn notify(&self, _: &str) {}
    async fn channel_history(&self, _: u16) -> Result<String, String> {
        Err("Discord is deliberately unavailable in this local smoke test".into())
    }
    async fn thread_history(&self, _: ThreadId, _: u16) -> Result<String, String> {
        Err("Discord is deliberately unavailable in this local smoke test".into())
    }
    async fn thread_participants(&self) -> Result<String, String> {
        Err("Discord is deliberately unavailable in this local smoke test".into())
    }
    async fn upload_file(
        &self,
        _: omp_client_bridge::omp::UploadRequest,
    ) -> Result<String, String> {
        Err("Discord is deliberately unavailable in this local smoke test".into())
    }
    async fn send_channel_message(&self, _: &str) -> Result<String, String> {
        Err("Discord is deliberately unavailable in this local smoke test".into())
    }
    async fn send_thread_message(
        &self,
        _: &str,
        _: Option<ThreadId>,
    ) -> Result<String, String> {
        Err("Discord is deliberately unavailable in this local smoke test".into())
    }
    async fn convert_message_to_thread(
        &self,
        _: MessageId,
        _: Option<&str>,
    ) -> Result<String, String> {
        Err("Discord is deliberately unavailable in this local smoke test".into())
    }
}

fn turn(id: i64, content: &str) -> Turn {
    Turn {
        conversation_id: ConversationId::new(1).unwrap(),
        id,
        thread_id: ThreadId::new(1),
        discord_message_id: MessageId::new(id as u64),
        author_id: UserId::new(1),
        content: content.into(),
        status: TurnStatus::Running,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let workspace = temporary.path().join("workspace");
    let sessions = temporary.path().join("sessions");
    std::fs::create_dir_all(workspace.join(".omp"))?;
    std::fs::create_dir_all(&sessions)?;
    let overlay = workspace.join(".omp/config.yml");
    std::fs::write(&overlay, "tools:\n  approvalMode: yolo\n")?;
    let home = std::env::var("HOME")?;
    let mut config =
        AppConfig::load(&Path::new(&home).join(".config/omp-client-bridge/config.toml"))?.omp;
    config.allowed_root = workspace.clone();
    config.session_dir = sessions;
    let transport = OmpTransport::new(config, Duration::from_secs(180), 100_000, 1);
    transport.probe().await?;
    println!("PASS real OMP RPC startup and clean shutdown");
    let mut session = Session {
        conversation_id: ConversationId::new(1)?,
        thread_id: ThreadId::new(1),
        guild_id: GuildId::new(1),
        parent_channel_id: ChannelId::new(1),
        trigger_message_id: MessageId::new(1),
        omp_session_path: None,
        model: None,
        cwd: workspace.clone(),
        created_by_user_id: UserId::new(1),
        status: SessionStatus::Active,
        silent: false,
    };
    let ui = Ui {
        command_started: Notify::new(),
        approval_started: Notify::new(),
    };
    let control = Control::default();
    let interrupts = Arc::new(Notify::new());
    let first = turn(
        1,
        "Local transport acceptance test. Remember marker COBALT-7391 for this conversation. Now use bash to run `sleep 45` in the foreground. Do not delegate or run it in the background. After it finishes reply SLEEP_FINISHED.",
    );
    let run = transport.run_turn(
        &session,
        TurnInput {
            first: &first,
            interrupts: interrupts.clone(),
            control: &control,
        },
        CancellationToken::new(),
        &ui,
    );
    let replacement = async {
        tokio::time::timeout(Duration::from_secs(90), ui.command_started.notified()).await?;
        {
            let mut queue = control.queue.lock().unwrap();
            queue.push_back(turn(2, "Interrupt the sleep. Reply INTERMEDIATE."));
            queue.push_back(turn(3, "Interrupt again. Do not run tools. Reply with only the original COBALT marker from this conversation."));
        }
        interrupts.notify_one();
        Ok::<(), anyhow::Error>(())
    };
    let (answer, ()) = tokio::try_join!(async { Ok::<_, anyhow::Error>(run.await?) }, replacement)?;
    ensure!(
        answer.turn_id == 3 && !answer.aborted && answer.text.contains("COBALT-7391"),
        "newest replacement did not preserve the conversation: {:?}",
        answer
    );
    {
        let outcomes = control.results.lock().unwrap();
        ensure!(
            outcomes.contains(&(1, TurnStatus::Aborted)),
            "initial command did not abort: {outcomes:?}"
        );
        ensure!(
            outcomes.contains(&(3, TurnStatus::Done)),
            "replacement did not complete: {outcomes:?}"
        );
        ensure!(
            outcomes.iter().any(|(id, _)| *id == 2),
            "intermediate replacement lost: {outcomes:?}"
        );
        println!(
            "PASS real foreground command interruption with two rapid replacements: {outcomes:?}"
        );
    }
    session.omp_session_path = Some(answer.session_path);
    let resume_control = Control::default();
    let resume = turn(
        4,
        "Without tools, reply with only the original COBALT marker stored earlier in this conversation.",
    );
    let answer = transport
        .run_turn(
            &session,
            TurnInput {
                first: &resume,
                interrupts: Arc::new(Notify::new()),
                control: &resume_control,
            },
            CancellationToken::new(),
            &ui,
        )
        .await?;
    ensure!(
        answer.text.contains("COBALT-7391") && !answer.aborted,
        "resumed process lost conversation marker"
    );
    println!("PASS separate real OMP process resumes the saved conversation marker");
    session.omp_session_path = Some(answer.session_path);
    std::fs::write(&overlay, "tools:\n  approvalMode: always-ask\n")?;
    let approval_control = Control::default();
    let approval_interrupts = Arc::new(Notify::new());
    let approval = turn(
        5,
        "Use bash to run exactly `printf forbidden > approval-must-not-exist` in the working directory. This local acceptance test expects an approval prompt. Do not use another tool or delegate.",
    );
    let run = transport.run_turn(
        &session,
        TurnInput {
            first: &approval,
            interrupts: approval_interrupts.clone(),
            control: &approval_control,
        },
        CancellationToken::new(),
        &ui,
    );
    let replacement = async {
        tokio::time::timeout(Duration::from_secs(90), ui.approval_started.notified()).await?;
        approval_control.queue.lock().unwrap().push_back(turn(
            6,
            "Cancel that unapproved command. Do not use any tools. Reply only APPROVAL_CANCELLED.",
        ));
        approval_interrupts.notify_one();
        Ok::<(), anyhow::Error>(())
    };
    let (answer, ()) = tokio::try_join!(async { Ok::<_, anyhow::Error>(run.await?) }, replacement)?;
    ensure!(
        answer.turn_id == 6 && answer.text.contains("APPROVAL_CANCELLED") && !answer.aborted,
        "approval replacement did not finish"
    );
    ensure!(
        !workspace.join("approval-must-not-exist").exists(),
        "unapproved command executed"
    );
    ensure!(
        approval_control
            .results
            .lock()
            .unwrap()
            .contains(&(5, TurnStatus::Aborted)),
        "approval predecessor did not abort"
    );
    println!("PASS unanswered approval interruption; forbidden file was not created");
    println!(
        "No Discord API calls. Existing host OMP profile used; temporary workspaces and session files removed on exit."
    );
    Ok(())
}
