# Working memory

Working memory is the request-local context the host uses to generate a response. Nextral assembles session tail, summary, enabled procedural policies and retrieved memory; the host owns model reasoning, tools, prompt construction and disposal.

`nextral.session.context` accepts scope, session and retrieval inputs, including the host query vector. Its token estimate accounts for the assembled layers. It is not proof of exact provider-tokenizer consumption and does not persist private chain-of-thought.

Use explicit ingestion if a tool result or decision should become durable memory. Do not assume every transient observation is automatically extracted or saved. A working-context response is not a replacement for authorizing the caller or managing host prompt secrets.
