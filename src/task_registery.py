import yagmail
import random
import os
from dotenv import load_dotenv
load_dotenv()
email= os.environ.get('EMAIL')
password = os.environ.get('EMAIL_PASSWORD')
yag = yagmail.SMTP(email, password)


def matrix_multiply(size):

    A = [[random.random() for _ in range(size)] for _ in range(size)]
    B = [[random.random() for _ in range(size)] for _ in range(size)]

    result = [[0] * size for _ in range(size)]

    for i in range(size):
        for j in range(size):
            for k in range(size):
                result[i][j] += A[i][k] * B[k][j]

    return result

def send_email(email, subject, body):
    yag.send(to=email, subject=subject, contents=body)
    return {"result":f"Email sended to {email} "}

TASKS={
    # Dynamic dispatch to handle multiple tasks
    "send_email": send_email,
    "matrix_multiply":matrix_multiply,
}
