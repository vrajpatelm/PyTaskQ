import os
import re

def replace_in_file(path, replacements):
    if not os.path.exists(path): return
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    
    for old, new in replacements:
        content = content.replace(old, new)
        
    # Also regex replacements
    content = re.sub(r'f"dlq:\{TEST_TENANT_ID\}"', '"dlq"', content)
    content = re.sub(r'f"stats:pending:\{TEST_TENANT_ID\}"', '"stats:pending"', content)
    content = re.sub(r'f"stats:processing:\{TEST_TENANT_ID\}"', '"stats:processing"', content)
    content = re.sub(r'f"stats:delayed:\{TEST_TENANT_ID\}"', '"stats:delayed"', content)
    content = re.sub(r'f"stats:completed:\{TEST_TENANT_ID\}"', '"stats:completed"', content)
    content = re.sub(r'f"stats:failed:\{TEST_TENANT_ID\}"', '"stats:failed"', content)
    content = re.sub(r'f"stats:dlq:\{TEST_TENANT_ID\}"', '"stats:dlq"', content)
    content = re.sub(r'f"idempotency:\{TEST_TENANT_ID\}', 'f"idempotency:global', content)
    
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

replace_in_file("tests/test_worker_handle_task.py", [
    ('"tenant_id": TEST_TENANT_ID,', ''),
    ('"client_ip": "127.0.0.1",', ''),
    ('assert result.get("tenant_id") == TEST_TENANT_ID', ''),
])

replace_in_file("tests/test_dlq_endpoints.py", [
    ('headers=AUTH_HEADERS', ''),
    ('from conftest import AUTH_HEADERS, TEST_API_KEY, TEST_TENANT_ID', 'from conftest import TEST_TENANT_ID'),
    ('await fake_redis_for_app.hset(f"api_key:{TEST_API_KEY}"', '#'),
])

replace_in_file("tests/test_task_queuing.py", [
    ('headers=AUTH_HEADERS', ''),
    ('from conftest import AUTH_HEADERS, TEST_API_KEY, TEST_TENANT_ID', 'from conftest import TEST_TENANT_ID'),
    ('assert task["tenant_id"] == TEST_TENANT_ID', ''),
    ('assert "client_ip" in task', ''),
])

replace_in_file("tests/conftest.py", [
    ('"client_ip": "127.0.0.1",', ''),
])
