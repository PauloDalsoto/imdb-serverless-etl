import subprocess
import sys
import os

def run_command(command, cwd=None):
    print(f"\n[+] Running: {command}")
    result = subprocess.run(command, shell=True, cwd=cwd)
    if result.returncode != 0:
        print(f"[!] Command failed: {command}")
        sys.exit(result.returncode)

def main():
    root_dir = os.path.dirname(os.path.abspath(__file__))

    # Step 1: Build the SAM application
    run_command("sam build", cwd=root_dir)

    # Step 2: Deploy the stack using samconfig.toml
    run_command("sam deploy", cwd=root_dir)

    # Step 3: Post-deployment configuration (set up S3 trigger)
    run_command(f"{sys.executable} utils/setup_s3_trigger.py", cwd=root_dir)

    print("\n[✔] Deployment and post-configuration completed successfully!")

if __name__ == "__main__":
    main()
