import os
import re
import shutil

def copy_and_update():
    # Copy directory instead of renaming to bypass file locks
    if os.path.exists("src") and not os.path.exists("pytaskq"):
        shutil.copytree("src", "pytaskq")
        
    def process_file(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Replace imports
        new_content = re.sub(r'from src\.', 'from pytaskq.', content)
        new_content = re.sub(r'from pytaskq ', 'from pytaskq ', new_content)
        new_content = re.sub(r'import pytaskq\.', 'import pytaskq.', new_content)
        new_content = re.sub(r'import pytaskq\b', 'import pytaskq', new_content)
        
        if new_content != content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(new_content)
                print(f"Updated {filepath}")

    for root, dirs, files in os.walk("."):
        if ".venv" in root or ".git" in root or ".gemini" in root or "src" in root.split(os.sep):
            continue
        for file in files:
            if file.endswith(".py") or file.endswith(".md"):
                process_file(os.path.join(root, file))

if __name__ == "__main__":
    copy_and_update()
