def status_topic(step_type: str) -> str:
    return f"status.{step_type}.v1"

def reply_topic() -> str:
    return "reply.tasks.v1"

def query_topic() -> str:
    return "query.tasks.v1"

def worker_announce_topic() -> str:
    return "workers.announce.v1"

def cmd_topic(step_type: str) -> str:
    return f"cmd.{step_type}.v1"
