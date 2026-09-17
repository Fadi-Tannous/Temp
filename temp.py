
import json
import boto3
import pandas as pd
from botocore.exceptions import ClientError

def get_all_sagemaker_models(region_name: str = None) -> list[dict]:
    sm_client = boto3.client("sagemaker", region_name=region_name)
    paginator = sm_client.get_paginator("list_models")

    model_records = []

    print("Fetching models from AWS SageMaker...")
    for page in paginator.paginate():
        for summary in page.get("Models", []):
            model_name = summary["ModelName"]
            model_arn = summary["ModelArn"]
            creation_time = summary["CreationTime"]

            # Fetch detailed metadata
            try:
                desc = sm_client.describe_model(ModelName=model_name)
            except ClientError as e:
                print(f"Warning: Failed to describe model {model_name}: {e}")
                continue

            # Fetch tags
            tags_dict = {}
            try:
                tag_paginator = sm_client.get_paginator("list_tags")
                for tag_page in tag_paginator.paginate(ResourceArn=model_arn):
                    for tag in tag_page.get("Tags", []):
                        tags_dict[tag["Key"]] = tag["Value"]
            except ClientError as e:
                print(f"Warning: Failed to fetch tags for {model_name}: {e}")

            # Extract Primary Container or Containers (inference pipelines)
            primary_container = desc.get("PrimaryContainer", {})
            containers = desc.get("Containers", [])
            
            # Aggregate container image URI(s) and ModelDataUrl(s)
            image_uris = []
            model_data_urls = []

            if primary_container:
                if "Image" in primary_container:
                    image_uris.append(primary_container["Image"])
                if "ModelDataUrl" in primary_container:
                    model_data_urls.append(primary_container["ModelDataUrl"])
            
            for c in containers:
                if "Image" in c:
                    image_uris.append(c["Image"])
                if "ModelDataUrl" in c:
                    model_data_urls.append(c["ModelDataUrl"])

            # Flatten record
            record = {
                "ModelName": model_name,
                "ModelArn": model_arn,
                "CreationTime": creation_time.strftime("%Y-%m-%d %H:%M:%S") if creation_time else "",
                "ExecutionRoleArn": desc.get("ExecutionRoleArn", ""),
                "ImageURI": "; ".join(image_uris),
                "ModelDataUrl": "; ".join(model_data_urls),
                "EnableNetworkIsolation": desc.get("EnableNetworkIsolation", False),
                "VpcSecurityGroupIds": "; ".join(desc.get("VpcConfig", {}).get("SecurityGroupIds", [])),
                "VpcSubnets": "; ".join(desc.get("VpcConfig", {}).get("Subnets", [])),
                "Tags_JSON": json.dumps(tags_dict) if tags_dict else "",
            }

            # Add each tag as its own column prefixed with 'Tag:'
            for k, v in tags_dict.items():
                record[f"Tag:{k}"] = v

            model_records.append(record)

    return model_records

def main():
    # Pass a region like 'us-east-1' or leave None to use default configured region
    region = None 
    output_filename = "sagemaker_models_metadata.xlsx"

    records = get_all_sagemaker_models(region_name=region)

    if not records:
        print("No SageMaker models found.")
        return

    df = pd.DataFrame(records)

    # Save to Excel with openpyxl engine
    with pd.ExcelWriter(output_filename, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Models")

    print(f"Exported {len(records)} models to {output_filename}")

if __name__ == "__main__":
    main()
