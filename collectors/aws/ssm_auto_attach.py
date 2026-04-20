import boto3
from botocore.exceptions import ClientError
import time

REGION = "us-east-1"
TARGET_INSTANCE_PROFILE_NAME = "ec2-ssm-profile"
SSM_READY_POLL_INTERVAL = 10
SSM_READY_MAX_WAIT = 120

ec2 = boto3.client("ec2", region_name=REGION)
iam = boto3.client("iam")
ssm = boto3.client("ssm", region_name=REGION)


def get_target_profile_arn(profile_name: str) -> str:
    """Fetch ARN for the target instance profile, raising clearly if absent."""
    try:
        profile = iam.get_instance_profile(InstanceProfileName=profile_name)["InstanceProfile"]
        print(f"[OK] Found instance profile: {profile['InstanceProfileName']}")
        return profile["Arn"]
    except iam.exceptions.NoSuchEntityException:
        raise RuntimeError(
            f"Instance profile '{profile_name}' does not exist. "
            "Create it with the AmazonSSMManagedInstanceCore policy before running this script."
        )


def get_association_id(instance_id: str) -> str | None:
    """Return the existing association ID for an instance, or None."""
    resp = ec2.describe_iam_instance_profile_associations(
        Filters=[{"Name": "instance-id", "Values": [instance_id]}]
    )
    assocs = resp.get("IamInstanceProfileAssociations", [])
    if assocs:
        return assocs[0]["AssociationId"]
    return None


def wait_for_ssm_registration(instance_id: str) -> bool:
    """Poll until SSM reports the instance as Online, or timeout."""
    waited = 0
    while waited < SSM_READY_MAX_WAIT:
        resp = ssm.describe_instance_information(
            Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
        )
        info = resp.get("InstanceInformationList", [])
        if info and info[0].get("PingStatus") == "Online":
            return True
        time.sleep(SSM_READY_POLL_INTERVAL)
        waited += SSM_READY_POLL_INTERVAL
    return False


def attach_or_replace_instance_profile(
    instance_id: str,
    target_profile_arn: str,
) -> None:
    """
    Associate the target profile with the instance.
    If a different profile is already attached, replace it.
    If the correct profile is already attached, skip.
    """
    association_id = get_association_id(instance_id)

    if association_id:
        resp = ec2.describe_iam_instance_profile_associations(
            AssociationIds=[association_id]
        )
        current_arn = resp["IamInstanceProfileAssociations"][0]["IamInstanceProfile"]["Arn"]

        if current_arn == target_profile_arn:
            print(f"[SKIP]     {instance_id} — correct profile already attached")
            return

        print(f"[REPLACE]  {instance_id} — replacing {current_arn}")
        ec2.replace_iam_instance_profile_association(
            AssociationId=association_id,
            IamInstanceProfile={"Name": TARGET_INSTANCE_PROFILE_NAME},
        )
    else:
        ec2.associate_iam_instance_profile(
            InstanceId=instance_id,
            IamInstanceProfile={"Name": TARGET_INSTANCE_PROFILE_NAME},
        )
        print(f"[ATTACHED] {instance_id}")

    print(f"[WAITING]  {instance_id} — polling for SSM registration...")
    if wait_for_ssm_registration(instance_id):
        print(f"[READY]    {instance_id} — SSM agent online")
    else:
        print(f"[TIMEOUT]  {instance_id} — SSM agent not online after {SSM_READY_MAX_WAIT}s")


def ensure_ssm_profile_attached_to_all_instances() -> None:
    """Attach or replace the SSM instance profile on every non-terminated EC2 instance."""
    target_profile_arn = get_target_profile_arn(TARGET_INSTANCE_PROFILE_NAME)

    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate():
        for reservation in page["Reservations"]:
            for instance in reservation["Instances"]:
                instance_id = instance["InstanceId"]
                state = instance.get("State", {}).get("Name", "")

                if state in ("terminated", "shutting-down"):
                    continue

                try:
                    attach_or_replace_instance_profile(instance_id, target_profile_arn)
                except ClientError as e:
                    print(f"[ERROR]    {instance_id} — {e.response['Error']['Code']}: "
                          f"{e.response['Error']['Message']}")

