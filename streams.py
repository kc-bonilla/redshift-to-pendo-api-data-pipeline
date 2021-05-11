from singer.logger import get_logger
import simplejson as json

LOGGER = get_logger()
"""STREAMS:
key_properties: Primary key fields for identifying an endpoint record.
replication_method: INCREMENTAL or FULL_TABLE
replication_keys: bookmark_field(s), typically a date-time, used for filtering the results and setting the state
bookmark_type: Data type for bookmark, integer or datetime
"""

STREAMS = {
    "pendo_integration_account": {
        "stream_name": "pendo_integration_account",
        "target_entity": "accounts",
        "key_properties": ["platform_account_public_id"],
        "primary_key": "accountId",
        "replication_method": "FULL_TABLE",
        "replication_key": ["last_updated"],
        "bookmark_type": "datetime",
        "field_mappings": {
            "accountId": "platform_account_public_id",
            "platform_account_id": "platform_account_id",
            "sgaccountstatus": "sg_account_status",
            "sgphotoplanusedpercentage": "sg_photo_plan_used_percentage",
            "sglabpricesheetcreatedcount": "sg_lab_price_sheet_created_count",
            "sggallerieslabpricesheetassignedcount": "sg_galleries_lab_price_sheet_assigned_count",
            "sglabfulfilledordersapprovedcount": "sg_lab_fulfilled_orders_approved_count",
            "sgselffulfilledordersreceivedcount": "sg_self_fulfilled_orders_received_count",
            "sgaccountcountry": "sg_account_country",
            "sgtrusttier": "sg_trust_tier",
            "sgisfree": "sg_is_free",
            "sgisintrial": "sg_is_in_trial",
            "sglegacypaymentsmo": "sg_legacy_payments_12mo"
        }
    },
    "pendo_integration_visitor": {
        "stream_name": "pendo_integration_visitor",
        "target_entity": "visitors",
        "key_properties": ["platform_user_public_id"],
        "primary_key": "visitorId",
        "replication_method": "INCREMENTAL",
        "replication_key": ["last_updated"],
        "bookmark_type": "datetime",
        "field_mappings": {
            "visitorId": "platform_user_public_id",
            "sgaccountowner": "sg_account_owner"
        }
    }
 }


def get_stream_props(stream=None):
    """de-nest for creating user-specified catalog (catalog_from_config)"""
    stream_props = {}
    if stream not in STREAMS.keys():
        raise Exception(
            "Stream not defined as Pendo Stream"
        )
    elif stream in STREAMS.keys():
        for stream_name, stream_props in STREAMS.items():
            stream_props[stream_name] = {
                "stream_name": stream_props.get("stream_name"),
                "key_properties": stream_props.get("key_properties"),
                "replication_method": stream_props.get("replication_method"),
                "replication_key": stream_props.get("replication_key"),
                "primary_key": stream_props.get("primary_key"),
                "field_mappings": stream_props.get("field_mappings", True)
            }
    return stream_props
