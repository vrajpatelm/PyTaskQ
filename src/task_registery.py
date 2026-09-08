import numpy as np
import yagmail
import os
from dotenv import load_dotenv

load_dotenv()


def matrix_multiply(size: int):
    A = np.random.rand(size, size)
    B = np.random.rand(size, size)
    result = np.dot(A, B)
    # Return shape info only — returning a 1000x1000 float array as JSON
    # would produce a ~8MB response and crash Redis serialization.
    return f"Matrix multiplication complete: {size}x{size} result computed."


def send_email(email_to, subject, body):
    sender_email = os.environ.get('EMAIL')
    password = os.environ.get('EMAIL_PASSWORD')
    if not sender_email or not password:
        return {"result": f"Simulated email send to {email_to} (EMAIL credentials not set)"}

    yag = yagmail.SMTP(sender_email, password)
    yag.send(to=email_to, subject=subject, contents=body)
    return {"result": f"Email sent to {email_to}"}


TASKS = {
    # Dynamic dispatch to handle multiple tasks
    # Each entry defines the handler function AND its execution type.
    # The worker looks up both — the API never needs to know.
    # "send_email": {"handler": send_email, "type": "io"},  # Disabled: emails would send from platform owner's Gmail
    "matrix_multiply": {"handler": matrix_multiply, "type": "cpu"},
}
