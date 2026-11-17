FROM ghcr.io/local-inference-lab/vllm@sha256:745feb916ee816f5409fd58386114be4f22fbede7f9c0e0a7b8c4fe8dac0fb29
RUN python -m pip install --no-cache-dir soundfile==0.13.1 av==16.1.0
