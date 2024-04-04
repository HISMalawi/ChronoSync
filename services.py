import mysql.connector as mysql


def table_mapper(table_name):
    tables = [
        "drug_order",
        "encounter",
        "obs",
        "orders",
        "patient",
        "patient_identifier",
        "patient_program",
        "patient_state",
        "person",
        "person_address",
        "person_attribute",
        "person_name",
        "pharmacies",
        "pharmacy_batch_item_reallocations",
        "pharmacy_batch_items",
        "pharmacy_batches",
        "pharmacy_obs",
        "pharmacy_stock_balances",
        "pharmacy_stock_verifications",
        "relationship",
        "merge_audits",
        "note",
        "cohort_member"
    ]
    if table_name in tables:
        return True
    else:
        return False


def connectTest():
    try:
        db = mysql.connect(
            host="",
            user="",
            password="",
            database=""
        )
        return db

    except Exception as e:
        # logg the error
        print(e)
        return None
