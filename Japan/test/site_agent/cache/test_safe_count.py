import threading
import time

from web_agent.service import _safe_count_company_websites


def test_safe_count_company_websites_handles_concurrent_mutation():
    data = {str(i): f"site{i}.com" for i in range(2000)}
    stop_event = threading.Event()

    def mutator():
        toggle = False
        while not stop_event.is_set():
            # Mutate size within a small bound to avoid runaway growth.
            if toggle:
                data.pop("_extra", None)
            else:
                data["_extra"] = "x"
            toggle = not toggle

    thread = threading.Thread(target=mutator, daemon=True)
    thread.start()
    try:
        # Should not raise even if data changes during counting.
        for _ in range(50):
            count = _safe_count_company_websites(data)
            assert count >= 2000
            time.sleep(0.002)
    finally:
        stop_event.set()
        thread.join(timeout=1.0)
