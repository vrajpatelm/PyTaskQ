import yagmail
import random
import os
from dotenv import load_dotenv

load_dotenv()


def matrix_multiply(size):
    A = [[random.random() for _ in range(size)] for _ in range(size)]
    B = [[random.random() for _ in range(size)] for _ in range(size)]

    result = [[0] * size for _ in range(size)]

    for i in range(size):
        for j in range(size):
            for k in range(size):
                result[i][j] += A[i][k] * B[k][j]

    return result


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
    "send_email": {"handler": send_email, "type": "io"},
    "matrix_multiply": {"handler": matrix_multiply, "type": "cpu"},
}
