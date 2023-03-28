import json
import concurrent.futures
import traceback


class S3:
    def __init__(self, s3_client, s3_resource):
        self.s3_client = s3_client
        self.s3_resource = s3_resource

    # S3 get list of files in bucket
    def get_prefix_list_from_s3(self, bucket_name: str, prefix=""):

        s3_paginator = self.s3_client.get_paginator("list_objects_v2")
        output_files = []

        for page in s3_paginator.paginate(
            Bucket=bucket_name, Prefix=prefix, Delimiter="/"
        ):
            for d in page.get("Contents", ()):
                output_files.append(d["Key"])

        return output_files

    # S3 and JSON
    def load_json_from_s3_bucket(self, bucket_name, s3_filename):
        s3_resource = self.s3_resource
        content_object = s3_resource.Object(bucket_name, s3_filename)
        file_bytes = content_object.get()["Body"].read()
        file_content = file_bytes.decode("utf-8")
        obj = json.loads(file_content)
        return obj

    def save_json_to_s3_bucket(self, obj_to_save, bucket_name, key):
        self.s3_client.put_object(
            Body=json.dumps(obj_to_save), Bucket=bucket_name, Key=key
        )  # save to output


class Lambda:
    def __init__(self, lambda_client):
        self.lambda_client = lambda_client

    # Invoke lambdas
    def invoke_lambda(self, function_name, payload_dict):
        try:
            lambda_client = self.lambda_client

            payload = {"body": payload_dict}

            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",
                LogType="None",
                Payload=bytes(json.dumps(payload), encoding="utf8"),
            )

            res_payload = response.get("Payload").read()
            body = json.loads(res_payload).get("body")

            return body
        except:
            traceback.print_exc()

    def invoke_lambda_fire_and_forget(self, function_name, payload_dict):
        lambda_client = self.lambda_client
        try:
            payload = {"body": payload_dict}

            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="Event",
                LogType="None",
                Payload=bytes(json.dumps(payload), encoding="utf8"),
            )
        except:
            traceback.print_exc()

    def invoke_await_batched_lambdas(
        self, list_of_batched_payloads, max_concurrent_lambdas=200
    ):
        futures = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_concurrent_lambdas
        ) as executor:
            for idx, payload in enumerate(list_of_batched_payloads):
                future = executor.submit(self.invoke_lambda, payload_dict=payload)
                futures.append(future)

            # wait for batches to finish
            for future in concurrent.futures.as_completed(futures):
                response = future.result()
                if response == None:
                    print(f"ERROR: lambda returned Nonetype (didnt complete)")

                rd = json.loads(response)
                if rd["statusCode"] != 200:
                    message = rd["message"]
                    print(f"Error in lambda: {message}")
