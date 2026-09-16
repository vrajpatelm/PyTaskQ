import uuid

import numpy as np
import yagmail
import os
import csv
import random
import io
import time
import urllib
from PIL import Image
from dotenv import load_dotenv


load_dotenv()


def matrix_multiply(size: int):
    A = np.random.rand(size, size)
    B = np.random.rand(size, size)
    result = np.dot(A, B)
    # Return shape info only — returning a 1000x1000 float array as JSON
    # would produce a ~8MB response and crash Redis serialization.
    return f"Matrix multiplication complete: {size}x{size} result computed."


def generate_csv_report(rows: int):
    try:
        start_time = time.time()
        
        # 1. Create an in-memory string buffer
        output = io.StringIO()
        
        # 2. Create a CSV writer attached to the buffer
        writer = csv.writer(output)
        
        # 3. Write the header row
        writer.writerow(["transaction_id", "user_id", "amount", "status"])
        
        # 4. Generate the fake data
        for _ in range(rows):
            transaction_id = str(uuid.uuid4())
            user_id = random.randint(1, 500)
            amount = round(random.uniform(10.0, 1000.0), 2)
            status = random.choice(["Success", "Failed", "Pending"])
            
            writer.writerow([transaction_id, user_id, amount, status])
            
        # 5. Get the full CSV string from the buffer
        csv_string = output.getvalue()
        output.close()
        
        end_time = time.time()
        
        return {
            "status": "success",
            "rows_generated": rows,
            "bytes_size": len(csv_string),
            "time_taken_ms": round((end_time - start_time) * 1000)
        }
        
    except Exception as e:
        return {"status": "error", "error": str(e)}


def send_email(email_to, subject, body):
    sender_email = os.environ.get('EMAIL')
    password = os.environ.get('EMAIL_PASSWORD')
    if not sender_email or not password:
        return {"result": f"Simulated email send to {email_to} (EMAIL credentials not set)"}

    yag = yagmail.SMTP(sender_email, password)
    yag.send(to=email_to, subject=subject, contents=body)
    return {"result": f"Email sent to {email_to}"}


def resize_image(image_url: str, width: int, height: int):
    try:
        start_time = time.time()
        
        # 1. Ensure the output directory exists
        os.makedirs("thumbnails", exist_ok=True)
        
        # 2. Define where we will save the file locally
        filename = f"thumbnails/{time.time_ns()}.jpg"
        
        # 3. Download the image and save it temporarily
        urllib.request.urlretrieve(image_url, filename)
        
        # 4. Open, resize, and save the image using Pillow
        with Image.open(filename) as img:
            # Convert to RGB in case it's a PNG with transparency
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")
            
            resized = img.resize((width, height))
            resized.save(filename, format="JPEG", quality=85)
        
        end_time = time.time()
        
        return {
            "status": "success",
            "saved_path": filename,
            "original_url": image_url,
            "dimensions": f"{width}x{height}",
            "time_taken_ms": round((end_time - start_time) * 1000)
        }
        
    except Exception as e:
        return {"status": "error", "error": str(e)}
    
    
def url_health_check(url):
    try:
        time1=time.time()
        resp = urllib.request.urlopen(url,timeout=10) 
        time2 = time.time()
        diff = time2-time1
        return {"url": url, "status_code": resp.status, "response_time_ms": round(diff * 1000)}
    except Exception as e  :
        return {"url": url, "status_code": 0, "error": str(e)}
    
    

TASKS = {
    # Dynamic dispatch to handle multiple tasks
    # Each entry defines the handler function AND its execution type.
    # The worker looks up both — the API never needs to know.
    # "send_email": {"handler": send_email, "type": "io"},  # Disabled: emails would send from platform owner's Gmail
    "matrix_multiply": {"handler": matrix_multiply, "type": "cpu"},
    "url_health_check":{"handler":url_health_check,"type":"io"},
    "generate_csv_report":{"handler":generate_csv_report , "type":"cpu"},
    "resize_image": {"handler": resize_image, "type": "cpu"}
}
