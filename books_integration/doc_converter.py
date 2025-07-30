# Copyright (c) 2024, Wahni IT Solutions and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils import (
    flt, getdate, get_datetime_str, convert_utc_to_system_timezone, get_datetime
)
from books_integration.utils import get_doctype_name
from erpnext.accounts.doctype.journal_entry.journal_entry import get_default_bank_cash_account
from erpnext.accounts.party import get_party_account


class DocConverterBase:
    def __init__(self, instance, dirty_doc, target: str) -> None:
        self.doc_dict = dirty_doc
        if isinstance(self.doc_dict, Document):
            self.doc_dict = dirty_doc.as_dict()

        self.instance = instance
        self.field_map = getattr(self, 'field_map', {}) # Initialized by subclasses or empty dict
        self.converted_doc = {}
        self._dirty_doc = dirty_doc
        self.target = target
        self.source_doctype = self._dirty_doc.get("doctype")
        self.target_doctype = get_doctype_name(
            self.source_doctype, self.target, self._dirty_doc
        )
        self.doc_can_save = True
        self.doc_can_submit = True
        self.is_dict = isinstance(dirty_doc, dict)
        self.settings = frappe.get_cached_doc("Books Sync Settings")

        if self.target == "erpn":
            # Deep copy child_tables to prevent modifying the original class-level field_map
            child_tables_copy = self.field_map.pop("child_tables", [])[:]
            self.field_map = {v: k for k, v in self.field_map.items()} # Invert main field_map
            self.field_map["child_tables"] = child_tables_copy # Reassign the copied child tables


    def _convert_doc(self):
        if not self.field_map:
            return None

        self.converted_doc = {}
        self.converted_doc["doctype"] = self.target_doctype

        for field in self.doc_dict:
            fieldname = self._get_fieldname(field)

            if not fieldname:
                continue

            self.converted_doc[fieldname] = self.doc_dict.get(field)

        for child_table in (self.field_map.get("child_tables") or []):
            source_field = child_table.get("fbooks_fieldname")
            target_field = child_table.get("erpn_fieldname")

            if self.target != "erpn":
                source_field = child_table.get("erpn_fieldname")
                target_field = child_table.get("fbooks_fieldname")

            if not target_field:
                continue

            if not self.doc_dict.get(source_field):
                continue

            self.converted_doc[target_field] = []
            field_map = child_table.get("fieldmap")
            if self.target == "erpn":
                field_map = {v: k for k, v in field_map.items()} # Invert child table fieldmap

            for row in self.doc_dict.get(source_field):
                child_doc_item = {}
                for sfield, tfield in field_map.items():
                    child_doc_item[tfield] = row.get(sfield)
                self.converted_doc[target_field].append(child_doc_item)


    def _get_fieldname(self, field):
        if field in ("doctype", "fbooksDocName",):
            return None

        if not self.field_map:
            return None

        return self.field_map.get(field)

    def _fill_missing_values_for_fbooks(self):
        pass

    def _fill_missing_values_for_erpn(self):
        pass

    def get_converted_doc(self):
        self._convert_doc()

        if self.target == "erpn":
            self._fill_missing_values_for_erpn()
        else:
            self._fill_missing_values_for_fbooks()

        return self.converted_doc

    def get_frappe_doc(self):
        if not self.target == "erpn":
            return False

        if not self.converted_doc:
            self.get_converted_doc()

        return frappe.get_doc(self.converted_doc)

    def get_erp_payment_method(self, payment_method):
        methods = self.settings.get("mode_of_payment_mapping")
        if not methods:
            frappe.throw(_("Mode of Payment Mapping Not Set in Books Sync Settings"))
        for pay_method in methods:
            if pay_method.get("frappebooks_mode_of_payment") == payment_method:
                return pay_method.get("erpnext_mode_of_payment")
        frappe.throw(_(f"Mode of Payment '{payment_method}' Not Mapped in Books Sync Settings"))

    def get_item_tax_template(self, name: str, target: str):
        templates_map = {}

        sfield = "erpn_tax_template"
        tfield = "books_tax_template"
        if target == "erpn":
            sfield, tfield = tfield, sfield

        for row in (self.settings.get("tax_mapping") or []):
            templates_map[row.get(sfield)] = row.get(tfield)

        return templates_map.get(name)

    def get_erp_account_name(self, fbooks_account_name):
        """
        Gets the ERPNext account name based on FrappeBooks account name
        from the Books Sync Settings mapping.
        """
        account_maps = self.settings.get("account_mapping")
        if not account_maps:
            frappe.throw(_("Account Mapping Not Set in Books Sync Settings. Please configure it."))

        for account_map in account_maps:
            if account_map.get("frappebooks_account_name") == fbooks_account_name:
                return account_map.get("erpnext_account_name")

        if frappe.db.exists("Account", fbooks_account_name):
            return fbooks_account_name

        frappe.throw(_(f"ERPNext Account for '{fbooks_account_name}' not found in mapping or does not exist in ERPNext."))

    def get_erp_warehouse_name(self, fbooks_warehouse_name):
        """
        Gets the ERPNext warehouse name based on FrappeBooks warehouse name
        from the Books Sync Settings mapping.
        """
        warehouse_maps = self.settings.get("warehouse_mapping")
        if not warehouse_maps:
            frappe.throw(_("Warehouse Mapping Not Set in Books Sync Settings. Please configure it."))

        for warehouse_map in warehouse_maps:
            if warehouse_map.get("frappebooks_warehouse_name") == fbooks_warehouse_name:
                return warehouse_map.get("erpnext_warehouse_name")

        if frappe.db.exists("Warehouse", fbooks_warehouse_name):
            return fbooks_warehouse_name

        frappe.throw(_(f"ERPNext Warehouse for '{fbooks_warehouse_name}' not found in mapping or does not exist in ERPNext."))

    def get_erp_item_code(self, fbooks_item_name):
        """
        Gets the ERPNext Item Code based on FrappeBooks item name
        from the Books Sync Settings mapping.
        """
        item_maps = self.settings.get("item_mapping")
        if not item_maps:
            frappe.throw(_("Item Mapping Not Set in Books Sync Settings. Please configure it."))

        for item_map in item_maps:
            if item_map.get("frappebooks_item_name") == fbooks_item_name:
                return item_map.get("erpnext_item_code")

        if frappe.db.exists("Item", fbooks_item_name): # Fallback if FBooks name is already ERPNext code
            return fbooks_item_name

        frappe.throw(_(f"ERPNext Item Code for '{fbooks_item_name}' not found in mapping or does not exist in ERPNext."))


def init_doc_converter(instance, doc_dict, target: str):
    doctype = doc_dict.get("doctype")
    if doctype == "Item":
        return Item(instance, doc_dict, target)

    if doctype == "Customer":
        return Customer(instance, doc_dict, target)

    if doctype == "Supplier":
        return Supplier(instance, doc_dict, target)

    if doctype in ("Sales Invoice", "SalesInvoice",):
        return SalesInvoice(instance, doc_dict, target)

    if doctype in ("Payment Entry", "Payment",):
        return PaymentEntry(instance, doc_dict, target)

    if doctype in ("Stock Entry", "StockMovement",):
        return StockEntry(instance, doc_dict, target)

    if doctype in ("Price List", "PriceList",):
        return PriceList(instance, doc_dict, target)

    if doctype in ("Serial No", "SerialNumber",):
        return SerialNumber(instance, doc_dict, target)

    if doctype == "Batch":
        return Batch(instance, doc_dict, target)

    if doctype == "UOM":
        return UOM(instance, doc_dict, target)

    if doctype in ("UOM Conversion Detail", "UOMConversionItem"):
        return UOMConversionDetail(instance, doc_dict, target)

    if doctype in ("Delivery Note", "Shipment"):
        return DeliveryNote(instance, doc_dict, target)

    if doctype == "Address":
        return Address(instance, doc_dict, target)

    if doctype == "POSOpeningShift":
        return POSOpeningShift(instance, doc_dict, target)

    if doctype == "POSClosingShift":
        return POSClosingShift(instance, doc_dict, target)

    if doctype == "Pricing Rule":
        return PricingRule(instance, doc_dict, target)

    if doctype == "Item Group":
        return ItemGroup(instance, doc_dict, target)
    return False


class Item(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "image": "image",
            "item_code": "itemCode",
            "item_name": "name",
            "stock_uom": "unit",
            "description": "description",
            "gst_hsn_code": "hsnCode",
            "is_stock_item": "trackItem",
            "has_batch_no": "hasBatch",
            "has_serial_no": "hasSerialNumber",
            "child_tables": [
                {
                    "erpn_fieldname": "uoms",
                    "fbooks_fieldname": "uomConversions",
                    "fbooks_doctype": "UOMConversionItem",
                    "erpn_doctype": "UOM Conversion Detail",
                    "fieldmap": {"uom": "uom", "conversion_factor": "conversionFactor"},
                }
            ],
        }
        super().__init__(instance, dirty_doc, target)


    def _fill_missing_values_for_fbooks(self):
        if self.settings.sync_item_as_non_inventory:
            self.converted_doc["trackItem"] = 0

        if self.doc_dict.get("taxes"):
            self.converted_doc["tax"] = self.get_item_tax_template(
                self.doc_dict.get("taxes")[0]["item_tax_template"], self.target
            )

        if barcodes := self.doc_dict.get("barcodes"):
            self.converted_doc['barcode'] = barcodes[0].get("barcode")

        all_uoms = frappe.db.get_all(
            "UOM", filters={"must_be_whole_number": 1}, pluck="name"
        )
        for row in self.converted_doc["uomConversions"]:
            if row.get("uom") in all_uoms:
                row.setdefault("isWhole", True)
            else:
                row.setdefault("isWhole", False)

        self.converted_doc["hasBatch"] = bool(self.converted_doc["hasBatch"])

        self.converted_doc["itemGroup"] = self.doc_dict.get("item_group")


    def _fill_missing_values_for_erpn(self):
        self.converted_doc["name"] = self._dirty_doc.get("name")
        self.converted_doc["item_group"] = "Products"

        if self._dirty_doc.get("tax"):
            self.converted_doc["taxes"] = []
            self.converted_doc["taxes"].append(
                {
                    "item_tax_template": self.get_item_tax_template(
                        self._dirty_doc.get("tax"), self.target
                    )
                }
            )

        if self.doc_dict.get("barcode"):
            self.converted_doc["barcodes"] = []
            self.converted_doc["barcodes"].append(
                {"barcode": str(self._dirty_doc.get("barcode"))}
            )


class Customer(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "name": "name",
            "gstin": "gstin",
            "gst_category": "gstType",
            "customer_primary_address": "address",
        }
        super().__init__(instance, dirty_doc, target)

    def _fill_missing_values_for_erpn(self):
        self.converted_doc["customer_name"] = self._dirty_doc.get("name")
        address_name = frappe.db.get_value(
            "Books Reference",
            {
                "document_type": "Address",
                "books_name": self.converted_doc["customer_primary_address"],
                "books_instance": self.instance
            },
            "document_name"
        )
        self.converted_doc["customer_primary_address"]= address_name

    def _fill_missing_values_for_fbooks(self):
        self.converted_doc["role"] = "Customer"


class Supplier(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "name": "name",
            "gstin": "gstin",
            "gst_category": "gstType",
            "supplier_primary_address": "address",
        }
        super().__init__(instance, dirty_doc, target)

    def _fill_missing_values_for_erpn(self):
        self.converted_doc["supplier_name"] = self._dirty_doc.get("name")

        address_name = frappe.db.get_value(
            "Books Reference",
            {
                "document_type": "Address",
                "books_name": self.converted_doc["supplier_primary_address"],
                "books_instance": self.instance
            },
            "document_name"
        )
        self.converted_doc["supplier_primary_address"]= address_name

    def _fill_missing_values_for_fbooks(self):
        self.converted_doc["role"] = "Supplier"


class SalesInvoice(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "customer": "party",
            "posting_date": "date",
            "is_return": "isReturn",
            "return_against": "returnAgainst",
            "selling_price_list": "priceList",
            "net_total": "netTotal",
            "base_grand_total": "baseGrandTotal",
            "grand_total": "grandTotal",
            "currency": "currency",
            "conversion_rate": "exchangeRate",
            "outstanding_amount": "outstandingAmount",
            "terms": "terms",
            "child_tables": [
                {
                    "erpn_fieldname": "items",
                    "fbooks_fieldname": "items",
                    "fbooks_doctype": "SalesInvoiceItem",
                    "erpn_doctype": "Sales Invoice Item",
                    "fieldmap": {
                        # KEEPING THIS AS IS, AS YOU STATED IT WORKS FOR SALES INVOICE
                        # This means FBooks 'item' populates both ERPNext 'item_code' and 'item_name'
                        "item_code": "item",
                        "item_name": "item",
                        "description": "description",
                        "qty": "quantity",
                        "stock_uom": "unit",
                        "batch_no": "batch",
                        "conversion_factor": "unitConversionFactor",
                        "discount_percentage": "itemDiscountPercent",
                        "discount_amount": "itemDiscountAmount",
                        "price_list_rate": "rate",
                        "amount": "amount",
                        "income_account": "account", 
                    },
                },
            ],
        }
        super().__init__(instance, dirty_doc, target)

    def _fill_missing_values_for_erpn(self):
        self.converted_doc["disable_rounded_total"] = 1
        self.converted_doc["set_posting_time"] = 1
        self.converted_doc["posting_date"] = getdate(
            self.converted_doc["posting_date"]
        )
        pos_profile = frappe.db.get_value(
            "Books Instance", self.instance, "pos_profile"
        )
        if not pos_profile:
            frappe.throw(_(("POS Profile not set in Books Instance {0}").format(self.instance)))

        pos_details = frappe.db.get_value(
            "POS Profile", pos_profile, ["company", "customer"], as_dict=True
        )
        self.converted_doc["is_pos"] = 1
        self.converted_doc["pos_profile"] = pos_profile
        self.converted_doc["company"] = pos_details.get("company")
        
        customer_name_in_erpn = frappe.db.get_value(
            "Books Reference",
            {"document_type": "Customer", "books_name": self.converted_doc["customer"], "books_instance": self.instance},
            "document_name"
        )
        if customer_name_in_erpn:
            self.converted_doc["customer"] = customer_name_in_erpn
        elif not frappe.db.exists("Customer", self.converted_doc["customer"]):
            frappe.throw(_(f"Customer '{self.converted_doc['customer']}' not found in ERPNext or mapped via Books Reference."))


        for item in self.converted_doc["items"]:
            # THIS SECTION REMAINS AS PER YOUR WORKING SALES INVOICE CODE
            # It implies item["item_code"] and item["item_name"] are correctly populated by fieldmap
            # or implicitly handled by ERPNext validation based on FBooks 'item'.

            item["price_list_rate"] = flt(item.get("price_list_rate"))
            if flt(item.get("discount_percentage")) > 0:
                item["discount_amount"] = flt(
                    (flt(item.get("price_list_rate")) * flt(item.get("discount_percentage")))
                    / 100
                )
            elif flt(item.get("discount_amount")):
                item["discount_percentage"] = (flt(item.get("discount_amount"))/ flt(item.get("price_list_rate"))) * 100

            item["rate"] = flt(item["price_list_rate"]) - flt(item.get("discount_amount", 0))

            if item.get("income_account"):
                item["income_account"] = self.get_erp_account_name(item["income_account"])
            else:
                default_income_account = frappe.db.get_value(
                    "Company", self.converted_doc["company"], "default_income_account"
                )
                if default_income_account:
                    item["income_account"] = default_income_account
                else:
                    frappe.throw(_("No income account provided for item and no default set for company."))

            if item.get("batch_no"):
                item.setdefault("use_serial_batch_fields", 1)

        if self.converted_doc["return_against"]:
            self.converted_doc["is_return"] = 1
            self.converted_doc["update_outstanding_for_self"] = 1
            self.converted_doc["update_billed_amount_in_delivery_note"] = 1
            erpn_invoice = frappe.db.get_value(
                "Books Reference",
                {"books_name": self.converted_doc["return_against"], "books_instance": self.instance},
                "document_name"
            )
            self.converted_doc["return_against"] = erpn_invoice

        self.converted_doc['books_instance'] = self.instance
        self.converted_doc['from_frappebooks'] = 1

    def _fill_missing_values_for_fbooks(self):
        if self._dirty_doc.get("docstatus") == 2:
            self.converted_doc["submitted"] = True
            self.converted_doc["cancelled"] = True
            return

        self.converted_doc["submitted"] = bool(self._dirty_doc.get("docstatus"))


class PaymentEntry(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "posting_date": "date",
            "payment_type": "paymentType",
            "mode_of_payment": "paymentMethod",
            "total_allocated_amount": "amount",
            "reference_no": "referenceId",
            "reference_date": "clearanceDate",
            "child_tables": [
                {
                    "erpn_fieldname": "references",
                    "fbooks_fieldname": "for",
                    "fbooks_doctype": "PaymentFor",
                    "erpn_doctype": "Payment Entry Reference",
                    "fieldmap": {
                        "reference_name": "referenceName",
                        "reference_doctype": "referenceType",
                        "total_amount": "amount",
                    },
                },
            ],
        }
        super().__init__(instance, dirty_doc, target)

    def _fill_missing_values_for_erpn(self):
        pos_profile = frappe.db.get_value(
            "Books Instance", self.instance, "pos_profile"
        )
        if not pos_profile:
            frappe.throw(_(("POS Profile not set in Books Instance {0}").format(self.instance)))

        pos_details = frappe.db.get_value(
            "POS Profile", pos_profile, ["company", "customer"], as_dict=True
        )
        self.converted_doc["company"] = pos_details.get("company")
        
        # FIX: Resolve Party (Customer/Supplier) from Books Reference or directly
        fbooks_party_name = self.doc_dict.get("party")
        party_name_in_erpn = frappe.db.get_value(
            "Books Reference",
            {"document_type": "Customer", "books_name": fbooks_party_name, "books_instance": self.instance},
            "document_name"
        )
        if not party_name_in_erpn: 
            if frappe.db.exists("Customer", fbooks_party_name):
                party_name_in_erpn = fbooks_party_name
            elif frappe.db.exists("Supplier", fbooks_party_name):
                party_name_in_erpn = fbooks_party_name
            else:
                frappe.throw(_(f"Party '{fbooks_party_name}' not found in ERPNext or mapped via Books Reference."))
        self.converted_doc["party"] = party_name_in_erpn


        self.converted_doc["mode_of_payment"] = self.get_erp_payment_method(
            self._dirty_doc.get("paymentMethod")
        )

        is_party_is_customer = frappe.db.exists("Customer", self.converted_doc["party"])

        if is_party_is_customer:
            self.converted_doc["party_type"] = "Customer"
        else:
            self.converted_doc["party_type"] = "Supplier" 

        self.converted_doc["received_amount"] = flt(
            self.converted_doc["total_allocated_amount"]
        )

        self.converted_doc["paid_amount"] = flt(
            self.converted_doc["total_allocated_amount"]
        )

        # FIX: Determine Bank/Cash Account based on Mode of Payment
        bank_cash_account = None
        try:
            bank_cash_account_data = get_default_bank_cash_account(
                self.converted_doc['company'],
                self._dirty_doc['paymentMethod'], 
                self.converted_doc['mode_of_payment'], 
                account=None
            )
            bank_cash_account = bank_cash_account_data.account
        except Exception as e:
            frappe.throw(_(f"Could not determine default bank/cash account for mode of payment '{self.converted_doc['mode_of_payment']}' in company '{self.converted_doc['company']}': {e}"))
        
        party_account = get_party_account(
            self.converted_doc['party_type'], self.converted_doc['party'], self.converted_doc['company']
        )
        self.converted_doc['paid_from'] = party_account
        self.converted_doc['paid_to'] = bank_cash_account 


        self.converted_doc["posting_date"] = getdate(
            self.converted_doc["posting_date"]
        )
        self.converted_doc["reference_date"] = getdate(
            self.converted_doc["reference_date"]
        )

        for row in self.converted_doc["references"]:
            reference_name_in_erpn = frappe.db.get_value(
                "Books Reference",
                {"books_name": row["reference_name"], "books_instance": self.instance},
                "document_name",
            )
            # FIX: Throw if reference document not found in ERPNext
            if not reference_name_in_erpn:
                frappe.throw(_(f"Reference document '{row['reference_name']}' not found in ERPNext for Payment Entry '{self.doc_dict.get('name')}'. Please ensure it's synced and submitted."))

            row["reference_name"] = reference_name_in_erpn
            row["reference_doctype"] = get_doctype_name(
                row["reference_doctype"], self.target
            )

            row["total_amount"] = float(row["total_amount"])
            row["allocated_amount"] = float(row["total_amount"])

            # FIX: check if sales invoice is returned, and adjust payment type/amounts
            if frappe.db.get_value("Sales Invoice", row["reference_name"], "return_against"):
                self.converted_doc["payment_type"] = "Pay" 
                temp = self.converted_doc["paid_from"]
                self.converted_doc["paid_from"] = self.converted_doc["paid_to"]
                self.converted_doc["paid_to"] = temp
                if row["allocated_amount"] > 0:
                    row["allocated_amount"] = -row["allocated_amount"]

        self.converted_doc['books_instance'] = self.instance
        self.converted_doc['from_frappebooks'] = 1


class StockEntry(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "name": "name",
            "stock_entry_type": "movementType",
            "posting_date": "date",
            "total_amount": "amount",
            "child_tables": [
                {
                    "erpn_fieldname": "items",
                    "fbooks_fieldname": "items",
                    "fbooks_doctype": "StockMovementItem",
                    "erpn_doctype": "Stock Entry Detail",
                    "fieldmap": {
                        "item_name": "item", # Map FBooks item (name) to ERPNext item_name. item_code will be resolved.
                        "s_warehouse": "fromLocation",
                        "t_warehouse": "toLocation",
                        "qty": "quantity",
                        "transfer_qty": "transferQuantity",
                        "uom": "transferUnit",
                        "stock_uom": "unit",
                        "conversion_factor": "unitConversionFactor",
                        "basic_rate": "rate",
                        "amount": "amount",
                        "serial_no": "serialNumber",
                    },
                }
            ],
        }
        super().__init__(instance, dirty_doc, target)

    def _fill_missing_values_for_erpn(self):
        if "Material" in self.converted_doc["stock_entry_type"]:
            entry_type = self.converted_doc["stock_entry_type"].split("Material")[1]
            self.converted_doc["stock_entry_type"] = "Material {}".format(entry_type)
            self.converted_doc["purpose"] = "Material {}".format(entry_type)
        
        # Add company field for Stock Entry
        pos_profile = frappe.db.get_value(
            "Books Instance", self.instance, "pos_profile"
        )
        if not pos_profile:
            frappe.throw(_(("POS Profile not set in Books Instance {0}").format(self.instance)))
        company = frappe.db.get_value("POS Profile", pos_profile, "company")
        if not company:
            frappe.throw(_(f"Company not set for POS Profile {pos_profile} for Stock Entry."))
        self.converted_doc["company"] = company

        for item in self.converted_doc["items"]:
            # FIX: Resolve item_code and item_name using the new helper
            fbooks_item_name = item.get("item_name")
            if not fbooks_item_name:
                frappe.throw(_(f"Stock Entry Item is missing item name from FrappeBooks in row {item.get('idx', 'N/A')}"))

            erp_item_code = self.get_erp_item_code(fbooks_item_name)
            item["item_code"] = erp_item_code
            item["item_name"] = frappe.db.get_value("Item", erp_item_code, "item_name") or fbooks_item_name


            # FIX: Apply warehouse mapping for source and target warehouses
            if item.get("s_warehouse"):
                item["s_warehouse"] = self.get_erp_warehouse_name(item["s_warehouse"])
            if item.get("t_warehouse"):
                item["t_warehouse"] = self.get_erp_warehouse_name(item["t_warehouse"])


    def _fill_missing_values_for_fbooks(self):
        if self._dirty_doc.get("docstatus") == 2: # Cancelled in ERPNext
            self.converted_doc["submitted"] = True 
            self.converted_doc["cancelled"] = True
        else:
            self.converted_doc["submitted"] = bool(self._dirty_doc.get("docstatus"))

        self.converted_doc["movementType"] = "".join(
            self.converted_doc["movementType"].split()
        )

        for item in self.converted_doc["items"]:
            serial_numbers_list = []
            
            if not item.get("use_serial_batch_fields") and item.get(
                "serial_and_batch_bundle"
            ):
                sn_batch_bundle = frappe.get_doc(
                    "Serial and Batch Bundle", item.get("serial_and_batch_bundle")
                )

                serial_numbers_list = [sn.get("serial_no") for sn in sn_batch_bundle.get("entries") if sn.get("serial_no")]
                item["serial_no"] = "\n".join(serial_numbers_list)

            else:
                item["serial_no"] = None


class PriceList(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "name": "name",
            "enabled": "isEnabled",
            "price_list_name": "name",
            "buying": "isPurchase",
            "selling": "isSelling",
            "child_tables": [
                {
                    "erpn_fieldname": "item_prices", 
                    "fbooks_fieldname": "priceListItem",
                    "fbooks_doctype": "PriceListItem",
                    "erpn_doctype": "Item Price",
                    "fieldmap": {
                        "name": "name",
                        "item_code": "item",
                        "uom": "unit",
                        "price_list": "parent",
                        "price_list_rate": "rate",
                    },
                }
            ],
        }
        super().__init__(instance, dirty_doc, target)


class ItemPrice(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "name": "name",
            "item_code": "item",
            "uom": "unit",
            "price_list": "parent",
            "price_list_rate": "rate",
        }
        super().__init__(instance, dirty_doc, target)

    def _fill_missing_values_for_fbooks(self):
        self.converted_doc["parentSchemaName"] = get_doctype_name(
            "Price List", "fbooks"
        )
        self.converted_doc["parentFieldname"] = "priceListItem"


class SerialNumber(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "serial_no": "name",
            "item_code": "item",
            "description": "description",
        }
        super().__init__(instance, dirty_doc, target)


class Batch(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "batch_id": "name",
            "item": "item",
            "expiry_date": "expiryDate",
            "manufacturing_date": "manufactureDate",
        }
        super().__init__(instance, dirty_doc, target)

    def _fill_missing_values_for_fbooks(self):
        self.converted_doc["item"] = frappe.db.get_value(
            "Item", self.converted_doc["item"], "item_name"
        )


class UOM(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "name": "name",
            "must_be_whole_number": "isWhole",
        }
        super().__init__(instance, dirty_doc, target)


class UOMConversionDetail(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {"uom": "uom", "conversion_factor": "conversionFactor"}
        super().__init__(instance, dirty_doc, target)


class DeliveryNote(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "customer": "party",
            "posting_date": "date",
            "grand_total": "grandTotal",
            "backReference": "against_sales_invoice", 
            "child_tables": [
                {
                    "erpn_fieldname": "items",
                    "fbooks_fieldname": "items",
                    "fbooks_doctype": "ShipmentItem",
                    "erpn_doctype": "Delivery Note Item",
                    "fieldmap": {
                        # FIX: Map FBooks 'item' (name) directly to ERPNext 'item_name'.
                        # 'item_code' will be resolved from 'item_name' in _fill_missing_values_for_erpn.
                        "item_name": "item", 
                        "qty": "quantity",
                        "uom": "unit",
                        "rate": "rate",
                        "warehouse": "location",
                    },
                }
            ],
        }
        super().__init__(instance, dirty_doc, target)

    def _fill_missing_values_for_erpn(self):
        pos_profile = frappe.db.get_value(
            "Books Instance", self.instance, "pos_profile"
        )
        if not pos_profile:
            frappe.throw(_(("POS Profile not set in Books Instance {0}").format(self.instance)))

        pos_details = frappe.db.get_value(
            "POS Profile", pos_profile, "company", as_dict=True
        )
        self.converted_doc["company"] = pos_details.get("company")

        self.converted_doc["posting_date"] = getdate(self.converted_doc["posting_date"])

        self.converted_doc['books_instance'] = self.instance
        self.converted_doc['from_frappebooks'] = 1

        # FIX: Resolve customer from Books Reference or directly exist in ERPNext
        customer_name_in_erpn = frappe.db.get_value(
            "Books Reference",
            {"document_type": "Customer", "books_name": self.converted_doc["customer"], "books_instance": self.instance},
            "document_name"
        )
        if customer_name_in_erpn:
            self.converted_doc["customer"] = customer_name_in_erpn
        elif not frappe.db.exists("Customer", self.converted_doc["customer"]):
            frappe.throw(_(f"Customer '{self.converted_doc['customer']}' from FBooks not found in ERPNext or mapped via Books Reference."))


        for row in self.converted_doc["items"]:
            # FIX: Resolve ERPNext item_code from the item_name received from FBooks
            fbooks_item_name = row.get("item_name") # This now holds "bangle" from FBooks 'item'
            if not fbooks_item_name:
                frappe.throw(_(f"Delivery Note Item is missing item name from FrappeBooks in row {row.get('idx', 'N/A')}"))
            
            erp_item_code = self.get_erp_item_code(fbooks_item_name) # Convert "bangle" to "BANGLE-001" (or whatever ERPNext code)

            row["item_code"] = erp_item_code # Set the actual ERPNext item_code for the Delivery Note item
            # row["item_name"] is already correctly populated by fieldmap.


            # FIX: Apply warehouse mapping for each item
            if row.get("warehouse"):
                row["warehouse"] = self.get_erp_warehouse_name(row["warehouse"])
            else:
                default_warehouse = frappe.db.get_value("Company", self.converted_doc["company"], "default_warehouse")
                if default_warehouse:
                    row["warehouse"] = default_warehouse
                else:
                    frappe.throw(_(f"No warehouse provided for item '{row.get('item_name')}' and no default set for company '{self.converted_doc['company']}' for Delivery Note."))

            # FIX: Handle Against Sales Invoice Item linking
            if self.doc_dict.get("backReference"):
                try:
                    reference_name_in_erpn = frappe.db.get_value(
                        "Books Reference",
                        {"books_name": self.doc_dict.get("backReference"), "books_instance": self.instance},
                        "document_name",
                    )

                    if reference_name_in_erpn:
                        row["against_sales_invoice"] = reference_name_in_erpn

                        # CRITICAL: Query using the RESOLVED item_code and ensure SI is submitted
                        inv_item_details = frappe.db.get_value(
                            "Sales Invoice Item",
                            {
                                "parent": reference_name_in_erpn,
                                "item_code": row["item_code"], # Use the RESOLVED ERPNext item_code here!
                                "docstatus": 1 # Ensure sales invoice item is from a submitted SI
                            },
                            ["name", "amount", "qty", "delivered_qty", "item_code"], # Get relevant fields for linking
                            as_dict=True
                        )

                        if inv_item_details:
                            row["si_detail"] = inv_item_details.name # This is what ERPNext requires for the link
                            row["billed_amt"] = inv_item_details.amount
                            # These fields are crucial for internal tracking by ERPNext
                            row["sales_invoice_item_code"] = inv_item_details.item_code
                            row["sales_invoice_qty"] = inv_item_details.qty
                            row["sales_invoice_delivered_qty"] = inv_item_details.delivered_qty

                        else:
                            frappe.log_error(f"Could not find matching Sales Invoice Item (code: '{row.get('item_code')}') in Sales Invoice '{reference_name_in_erpn}' for Delivery Note item. Ensure SI is submitted and item exists.", "Delivery Note Item Link Error")
                            frappe.throw(_(f"Item '{row.get('item_name')}' (ERPNext code: '{row.get('item_code')}') not found in Sales Invoice '{reference_name_in_erpn}' or Sales Invoice not submitted. Cannot create Delivery Note. Please check Sales Invoice items and their submission status."))
                    else:
                        frappe.log_error(f"Back Reference Sales Invoice '{self.doc_dict.get('backReference')}' not found in ERPNext via Books Reference for Delivery Note '{self.doc_dict.get('name')}'.", "Books Integration Delivery Note Sync")
                        frappe.throw(_(f"Sales Invoice '{self.doc_dict.get('backReference')}' not found in ERPNext Books Reference. Cannot create Delivery Note."))

                except Exception as e:
                    frappe.log_error(f"Error filling Delivery Note item details for backReference {self.doc_dict.get('backReference')}: {e}", "Books Integration Delivery Note Sync")
                    frappe.throw(_(f"Error processing item link for Delivery Note with Sales Invoice {self.doc_dict.get('backReference')}: {e}"))
            else:
                # If no backReference is provided, ensure 'against_sales_invoice' and 'si_detail' are not set.
                if "against_sales_invoice" in row: del row["against_sales_invoice"]
                if "si_detail" in row: del row["si_detail"]
                if "sales_invoice_item_code" in row: del row["sales_invoice_item_code"]
                if "sales_invoice_qty" in row: del row["sales_invoice_qty"]
                if "sales_invoice_delivered_qty" in row: del row["sales_invoice_delivered_qty"]


    def before_save(self):
        ref_doc_name = self._dirty_doc.get("backReference")
        if not ref_doc_name:
            self.doc_can_save = True
            return True

        ref_doc_name_in_erpn = frappe.db.get_value(
            "Books Reference", {"books_name": ref_doc_name, "books_instance": self.instance}, "document_name"
        )

        if not ref_doc_name_in_erpn:
            self.doc_can_save = False
            frappe.log_error(f"Delivery Note '{self._dirty_doc.get('name')}': Referenced Sales Invoice '{ref_doc_name}' not found in ERPNext for before_save check.", "Delivery Note Before Save")
            return False

        doc_status = frappe.db.get_value(
            "Sales Invoice", ref_doc_name_in_erpn, "docstatus"
        )

        if doc_status == 0: # If Sales Invoice is still Draft, Delivery Note shouldn't proceed
            self.doc_can_save = False
            frappe.log_error(f"Delivery Note '{self._dirty_doc.get('name')}': Referenced Sales Invoice '{ref_doc_name_in_erpn}' is still Draft. Delivery Note cannot be saved.", "Delivery Note Before Save")
            return False

        self.doc_can_save = True
        return True


class Address(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "name": "name",
            "address_line1": "addressLine1",
            "address_line2": "addressLine2",
            "city": "city",
            "state": "state",
            "country": "country",
            "pincode": "postalCode",
        }
        super().__init__(instance, dirty_doc, target)

    def _fill_missing_values_for_erpn(self):
        self.converted_doc["address_title"] = self.converted_doc.get("name")


class POSOpeningShift(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "period_start_date": "openingDate",
            "child_tables": [
                {
                    "erpn_fieldname": "balance_details",
                    "fbooks_fieldname": "openingAmounts",
                    "fbooks_doctype": "openingAmounts",
                    "erpn_doctype": "POS Opening Entry Detail",
                    "fieldmap": {
                        "mode_of_payment": "paymentMethod",
                        "opening_amount": "amount",
                    },
                },
            ],
        }
        super().__init__(instance, dirty_doc, target)

    def _fill_missing_values_for_erpn(self):
        pos_profile = frappe.db.get_value(
            "Books Instance", self.instance, "pos_profile"
        )
        if not pos_profile:
            frappe.throw(_(("POS Profile not set in Books Instance {0}").format(self.instance)))

        pos_details = frappe.db.get_value(
            "POS Profile", pos_profile, "company", as_dict=True
        )
        pos_user = frappe.db.get_value(
            "Books Instance", self.instance, "pos_user"
        )
        if not pos_user:
            frappe.throw(_(("POS User not set in Books Instance {0}").format(self.instance)))

        self.converted_doc["company"] = pos_details.get("company")
        self.converted_doc["pos_profile"] = pos_profile
        self.converted_doc['cashier'] = pos_user
        self.converted_doc['user'] = pos_user
        self.converted_doc["period_start_date"] = get_datetime_str(
            self.converted_doc["period_start_date"]
        )
        self.converted_doc['books_instance'] = self.instance
        self.converted_doc['from_frappebooks'] = 1
        
        for item in self.converted_doc['balance_details']:
            item["mode_of_payment"] = self.get_erp_payment_method(item.get("mode_of_payment"))

        self.converted_doc['balance_details'] = [
            item for item in self.converted_doc['balance_details']
            if flt(item.get("opening_amount")) != 0
        ]


class POSClosingShift(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "period_end_date": "closingDate",
            "pos_opening_entry": "openingShift",
            "child_tables": [
                {
                    "erpn_fieldname": "payment_reconciliation",
                    "fbooks_fieldname": "closingAmounts",
                    "fbooks_doctype": "closingAmounts",
                    "erpn_doctype": "POS Closing Entry Detail",
                    "fieldmap": {
                        "mode_of_payment": "paymentMethod",
                        "opening_amount": "openingAmount",
                        "closing_amount": "closingAmount",
                        "expected_amount": "expectedAmount",
                        "difference": "differenceAmount",
                    },
                },
            ],
        }
        super().__init__(instance, dirty_doc, target)

    def _fill_missing_values_for_erpn(self):
        pos_profile = frappe.db.get_value(
            "Books Instance", self.instance, "pos_profile"
        )
        if not pos_profile:
            frappe.throw(_(("POS Profile not set in Books Instance {0}").format(self.instance)))

        pos_details = frappe.db.get_value(
            "POS Profile", pos_profile, "company", as_dict=True
        )
        pos_user = frappe.db.get_value(
            "Books Instance", self.instance, "pos_user"
        )
        if not pos_user:
            frappe.throw(_(("POS User not set in Books Instance {0}").format(self.instance)))

        self.converted_doc["company"] = pos_details.get("company")
        self.converted_doc["pos_profile"] = pos_profile
        self.converted_doc['cashier'] = pos_user
        self.converted_doc['user'] = pos_user
        self.converted_doc["period_end_date"] = get_converted_datetime_str(
            self.converted_doc["period_end_date"]
        )
        self.converted_doc['books_instance'] = self.instance
        self.converted_doc['from_frappebooks'] = 1
        
        opening_entry = frappe.db.get_value(
            "Books Reference",
            {"books_name": self.converted_doc["pos_opening_entry"], "books_instance": self.instance},
            "document_name"
        )
        if not opening_entry:
             frappe.throw(_(f"POS Opening Entry reference not found for FrappeBooks name: {self.converted_doc['pos_opening_entry']}"))
        self.converted_doc["pos_opening_entry"] = opening_entry


        for item in self.converted_doc['payment_reconciliation']:
            item["mode_of_payment"] = self.get_erp_payment_method(item.get("mode_of_payment"))

        self.converted_doc['payment_reconciliation'] = [
            item for item in self.converted_doc['payment_reconciliation']
            if any(flt(item.get(key, 0)) != 0 for key in ["opening_amount", "closing_amount", "expected_amount", "difference"])
        ]


class PricingRule(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "title": "title",
            "price_or_product_discount": "discountType",
            "coupon_code_based": "isCouponCodeBased",
            "apply_multiple_pricing_rules": "isMultiple",
            "priority": "priority",
            "rate_or_discount": "priceDiscountType",
            "rate": "discountRate",
            "discount_percentage": "discountPercentage",
            "discount_amount": "discountAmount",
            "free_item": "freeItem",
            "free_qty": "freeItemQuantity",
            "free_item_uom": "freeItemUnit",
            "round_free_qty": "roundFreeItemQty",
            "is_recursive": "isRecursive",
            "recurse_for": "recurseEvery",
            "valid_from": "validFrom",
            "valid_upto": "validTo",
            "free_item_rate": "freeItemRate",
            "min_qty": "minQuantity",
            "max_qty": "maxQuantity",
            "min_amt": "minAmount",
            "max_amt": "maxAmount",
            "child_tables": [
                {
                    "erpn_fieldname": "items",
                    "fbooks_fieldname": "appliedItems",
                    "fbooks_doctype": "PricingRuleItem",
                    "erpn_doctype": "Pricing Rule Item Code",
                    "fieldmap": {
                        "item_code": "item", 
                        "uom": "unit",
                    },
                },
            ],
        }
        super().__init__(instance, dirty_doc, target)

    def _fill_missing_values_for_erpn(self):
        self.converted_doc["apply_on"] = "Item Code"
        self.converted_doc["has_priority"] = 1
        self.converted_doc["selling"] = 1 
        self.converted_doc["applicable_for"] = "Customer"
        self.converted_doc["for_price_list"] = self.settings.get("price_list")

        pos_profile = frappe.db.get_value(
            "Books Instance", self.instance, "pos_profile"
        )
        if not pos_profile:
            frappe.throw(_(("POS Profile not set in Books Instance {0}").format(self.instance)))

        customer = frappe.db.get_value(
            "POS Profile", pos_profile, "customer"
        )
        self.converted_doc["customer"] = customer

        company = frappe.db.get_value("POS Profile", pos_profile, "company")
        if not company:
            frappe.throw(_(f"Company not set for POS Profile {pos_profile}"))
        self.converted_doc["company"] = company

        for item in self.converted_doc.get("items", []):
            fbooks_item_value = item.get("item_code") 
            if fbooks_item_value:
                erp_item_code = self.get_erp_item_code(fbooks_item_value) 
                item["item_code"] = erp_item_code
            else:
                frappe.throw(_(f"Pricing Rule item is missing item_code from FrappeBooks in row {item.get('idx', 'N/A')}."))


    def _fill_missing_values_for_fbooks(self):
        self.converted_doc["isEnabled"] = 0 if self.doc_dict.get("disable") == 1 else 1
        
        self.converted_doc["appliedItems"] = convert_to_item_name(
            self.converted_doc["appliedItems"]
        )

        self.converted_doc["fbooksDocName"] = self.converted_doc["title"]

        if self.converted_doc["discountType"] == "Price":
            self.converted_doc["discountType"] = "Price Discount"
        elif self.converted_doc["discountType"] == "Product":
            self.converted_doc["discountType"] = "Product Discount"

        if self.converted_doc["priceDiscountType"] == "Discount Percentage":
            self.converted_doc["priceDiscountType"] = "percentage"
        elif self.converted_doc["priceDiscountType"] == "Discount Amount":
            self.converted_doc["priceDiscountType"] = "amount"
        elif self.converted_doc["priceDiscountType"] == "Rate":
            self.converted_doc["priceDiscountType"] = "rate"
        
        existing_ref = frappe.db.get_all(
            "Books Reference",
            filters={
                "document_name": self.doc_dict.get("name"),
                "document_type": "Pricing Rule",
                "books_instance": self.instance,
            },
            fields=["books_name"],
            pluck="books_name",
        )

        self.converted_doc["fbooksDocName"] = existing_ref[0] if existing_ref else self.doc_dict.get("books_name")
        self.converted_doc["erpnextDocName"] = self.doc_dict.get("name")

class ItemGroup(DocConverterBase):
    def __init__(self, instance, dirty_doc, target):
        self.field_map = {
            "name": "name",
            "gst_hsn_code": "hsnCode",
        }
        super().__init__(instance, dirty_doc, target)

    def _fill_missing_values_for_fbooks(self):
        if self.doc_dict.get("taxes") and self.doc_dict["taxes"] and self.doc_dict["taxes"][0]:
            tax = self.get_item_tax_template(
                self.doc_dict["taxes"][0].get("item_tax_template"), self.target
            )
            self.converted_doc["tax"] = tax


def get_converted_datetime_str(datetimestr):
    datetime_obj = get_datetime(datetimestr)
    datetime = convert_utc_to_system_timezone(datetime_obj)
    return get_datetime_str(datetime)

def convert_to_item_name(item_list):
    if not isinstance(item_list, list):
        frappe.log_error(f"Expected list for item_list, got {type(item_list)}", "Books Integration Item Name Conversion")
        return item_list

    all_items = frappe.db.get_all("Item", fields=["item_code", "item_name"])
    item_dict = {}
    for item in all_items:
        key = item.get("item_code")
        value = item.get("item_name")
        item_dict.setdefault(key, value)
    
    converted_list = []
    for row in item_list:
        if isinstance(row, dict) and "item" in row:
            row_copy = row.copy()
            row_copy["item"] = item_dict.get(row["item"], row["item"])
            converted_list.append(row_copy)
        else:
            converted_list.append(row)

    return converted_list