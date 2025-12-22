import boto3
import os
import zipfile
import json
import shutil
import uuid
from urllib.parse import urlparse
from datetime import datetime

# Assumptions for external dependencies based on context
# from your_app import EnvValues, query_dynamodb, update_dynamodb_entry

@staticmethod
def async_batch_job(job_data):
    """
    Executes the batch processing workflow:
    1. Download & Unzip -> 2. Process -> 3. Zip & Upload -> 4. Notify
    """
    print(f"Starting Job {job_data.get('id')}...")
    
    # --- Helper: Session Management ---
    def get_session(access_type, role_arn):
        """Creates a session, assuming a role if needed."""
        if access_type == "assume_role" and role_arn:
            sts = boto3.client('sts')
            creds = sts.assume_role(RoleArn=role_arn, RoleSessionName=f"Job-{job_data['id']}")['Credentials']
            return boto3.Session(
                aws_access_key_id=creds['AccessKeyId'],
                aws_secret_access_key=creds['SecretAccessKey'],
                aws_session_token=creds['SessionToken']
            )
        return boto3.Session()

    # --- Helper: Parse S3 URI ---
    def parse_s3(uri):
        p = urlparse(uri)
        return p.netloc, p.path.lstrip('/')

    # Setup Sessions
    # Note: DynamoDB & SNS usually use the Service's own Role (Env variables), 
    # while S3 Input/Output might use Customer Roles.
    service_session = boto3.Session() 
    input_session = get_session(job_data.get('inputAccessType'), job_data.get('inputAssumeRoleARN'))
    output_session = get_session(job_data.get('outputAccessType'), job_data.get('outputAssumeRoleARN'))

    # Local Scratch Paths
    job_id = job_data['id']
    base_dir = f"/tmp/{job_id}"
    input_dir = f"{base_dir}/input"
    extract_dir = f"{base_dir}/extracted"
    output_dir = f"{base_dir}/processed"
    os.makedirs(extract_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    try:
        # ====================================================
        # 1. Download Specified Zip from S3
        # ====================================================
        input_bucket, input_key = parse_s3(job_data['inputsS3Path'])
        local_zip_path = f"{input_dir}/input_data.zip"
        
        # Ensure input dir exists
        os.makedirs(input_dir, exist_ok=True)
        
        print(f"Downloading inputs from {input_bucket}/{input_key}...")
        input_session.client('s3').download_file(input_bucket, input_key, local_zip_path)

        # ====================================================
        # 2. Unzip File
        # ====================================================
        print("Unzipping files...")
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        # Get list of files to process (ignoring hidden files/folders)
        files_to_process = [
            f for f in os.listdir(extract_dir) 
            if os.path.isfile(os.path.join(extract_dir, f)) and not f.startswith('.')
        ]
        total_docs = len(files_to_process)

        # ====================================================
        # 3. Update DynamoDB Job (Specify Document Count)
        # ====================================================
        # Assuming helper function 'update_dynamodb_entry' exists
        # update_dynamodb_entry(EnvValues.BATCH_TABLE, job_id, {"total_doc_count": total_docs})
        print(f"Total documents to process: {total_docs}")

        # ====================================================
        # 4. Loop & Process (Extraction/Classification)
        # ====================================================
        processed_count = 0
        
        for filename in files_to_process:
            file_path = os.path.join(extract_dir, filename)
            
            # --- MOCK PROCESSING START ---
            # Replace this with your actual classification/extraction logic
            # result = run_inference(file_path, job_data['jobType'])
            result_data = f"Processed {filename} with jobType {job_data['jobType']}"
            
            # Write result to output directory
            with open(os.path.join(output_dir, f"{filename}.txt"), "w") as f:
                f.write(result_data)
            # --- MOCK PROCESSING END ---

            processed_count += 1
            
            # Update DynamoDB periodically (or every time if scale allows)
            # update_dynamodb_entry(EnvValues.BATCH_TABLE, job_id, {"completed_doc_count": processed_count})

        # ====================================================
        # 5. Zip Result and Store back to Output S3
        # ====================================================
        output_zip_name = f"results_{job_id}.zip"
        local_output_zip = f"{base_dir}/{output_zip_name}"
        
        print("Zipping results...")
        with zipfile.ZipFile(local_output_zip, 'w') as zipf:
            for root, _, files in os.walk(output_dir):
                for file in files:
                    zipf.write(
                        os.path.join(root, file), 
                        os.path.relpath(os.path.join(root, file), output_dir)
                    )

        # Upload to S3
        out_bucket, out_prefix = parse_s3(job_data['outputS3Path'])
        # Handle if outputS3Path is a folder or full key
        if not out_prefix.endswith('.zip'):
             out_key = f"{out_prefix.rstrip('/')}/{output_zip_name}"
        else:
             out_key = out_prefix

        print(f"Uploading results to {out_bucket}/{out_key}...")
        output_session.client('s3').upload_file(local_output_zip, out_bucket, out_key)
        final_s3_path = f"s3://{out_bucket}/{out_key}"

        # ====================================================
        # 6. Update DynamoDB (Completion)
        # ====================================================
        completion_update = {
            "status": "completed",
            "completed_s3_path": final_s3_path,
            "completed_at": datetime.utcnow().isoformat()
        }
        # update_dynamodb_entry(EnvValues.BATCH_TABLE, job_id, completion_update)

        # ====================================================
        # 7. Issue SNS Email Notification
        # ====================================================
        # Requires looking up the SNS Topic ARN for the App ID (Mocked here)
        sns_topic_arn = "arn:aws:sns:region:account:topic-name" 
        try:
            sns = service_session.client('sns')
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"Batch Job {job_id} Completed",
                Message=f"Your batch job for {job_data['appId']} is done.\nResults: {final_s3_path}"
            )
        except Exception as e:
            print(f"Warning: Failed to send SNS: {e}")

        # ====================================================
        # 8. Issue SQS Message (If configured)
        # ====================================================
        sqs_queue_url = job_data.get('completionSQSQueue') # Note: Assuming URL, convert if ARN
        if sqs_queue_url:
            try:
                # Use Output Session? Or Service Session? 
                # Usually Output Session if writing to Customer Queue.
                sqs = output_session.client('sqs')
                
                # If the provided value is an ARN, convert to URL first (Logic from previous turns)
                if sqs_queue_url.startswith("arn:"):
                     # Simple ARN to URL converter for this scope
                     parts = sqs_queue_url.split(':')
                     sqs_queue_url = f"https://sqs.{parts[3]}.amazonaws.com/{parts[4]}/{parts[5]}"

                sqs.send_message(
                    QueueUrl=sqs_queue_url,
                    MessageBody=json.dumps({
                        "jobId": job_id,
                        "status": "completed",
                        "output": final_s3_path
                    })
                )
            except Exception as e:
                print(f"Warning: Failed to send SQS: {e}")

        # ====================================================
        # 9. Create S3 Trigger File (If configured)
        # ====================================================
        trigger_path = job_data.get('completionS3TriggerPath')
        if trigger_path:
            try:
                trig_bucket, trig_key = parse_s3(trigger_path)
                # Ensure we have a valid object key, not just a folder
                if trig_key.endswith('/'):
                    trig_key += "_SUCCESS"
                
                output_session.client('s3').put_object(
                    Bucket=trig_bucket,
                    Key=trig_key,
                    Body=json.dumps({"jobId": job_id, "status": "completed"})
                )
            except Exception as e:
                print(f"Warning: Failed to create S3 trigger: {e}")

    except Exception as e:
        print(f"Job Failed: {e}")
        # update_dynamodb_entry(EnvValues.BATCH_TABLE, job_id, {"status": "failed", "error": str(e)})
        raise e
        
    finally:
        # Cleanup /tmp
        shutil.rmtree(base_dir, ignore_errors=True)
