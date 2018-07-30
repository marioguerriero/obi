# IMPORTANT
# Most of the code in this script is a just a "replica" of the one which
# can be found in the following GitLab folder:
# etl-backend/backend_sync/tree/master/processors
# Unfortunately there is no other way of being certain about the input size
# estimation if not by replicating the same actions each job takes

import datetime

from google.cloud import storage

get_table_list = \
    {
        'lh_de': ['public.api_apikey', 'public.auth_user',
                  'public.backend_crmdata', 'public.cart_order',
                  'public.cart_orderdeliveryevent',
                  'public.cart_orderdetailinquiry',
                  'public.cart_orderdetails',
                  'public.cart_orderedingredient', 'public.cart_orderitem',
                  'public.cart_thirdpartyorderinfo',
                  'public.geo_city', 'public.geo_country',
                  'public.geo_deliverypolygon', 'public.geo_district',
                  'public.geo_district_zipcodes', 'public.geo_street',
                  'public.geo_zipcode',
                  'public.newsletter_newslettersubscription',
                  'public.offers_offeravailability',
                  'public.offers_restaurantoffer',
                  'public.referrals_referral',
                  'public.restaurant_additionalingredient',
                  'public.restaurant_additionalingredientset',
                  'public.restaurant_additionalingredientset_menu_items',
                  'public.restaurant_menu', 'public.restaurant_menuitem',
                  'public.restaurant_menuitemtag',
                  'public.restaurant_menusection',
                  'public.restaurant_menusectionavailability',
                  'public.restaurant_menusectioncategory',
                  'public.restaurant_promotedrestaurantslot',
                  'public.restaurant_restaurant',
                  'public.restaurant_restaurantaddress',
                  'public.restaurant_restaurantavailability',
                  'public.restaurant_restaurantcategory',
                  'public.restaurant_restaurantcategorygroup',
                  'public.restaurant_restaurantcategorygrouprelation',
                  'public.restaurant_restaurantcategoryrelation',
                  'public.restaurant_restaurantchain',
                  'public.restaurant_restaurantexternalreference',
                  'public.restaurant_restaurantreachability',
                  'public.restaurant_restauranttag',
                  'public.restaurant_taggedmenuitem',
                  'public.restaurant_taggedrestaurant',
                  'public.restaurant_targetdistrict',
                  'public.shared_emailcontact',
                  'public.shared_phonecontact',
                  'public.urlhandler_nurl',
                  'public.coupon_campaign',
                  'public.coupon_campaign_restaurants',
                  'public.coupon_coupon', 'public.coupon_coupon_users',
                  'public.customer_customer',
                  'public.customer_customeraddress',
                  'public.customer_favouriterestaurant',
                  'public.payback_paybackcard',
                  'public.payback_paybackcardcustomer',
                  'public.payback_paybackcardorder',
                  'public.payback_paybacktransaction'],

        'pde_de': ['public.api_apikey', 'public.auth_user',
                   'public.backend_crmdata', 'public.cart_orderdeliveryevent',
                   'public.cart_orderdetailinquiry',
                   'public.cart_orderdetails',
                   'public.cart_orderedingredient',
                   'public.cart_thirdpartyorderinfo',
                   'public.coupon_campaign',
                   'public.coupon_campaign_restaurants',
                   'public.coupon_coupon', 'public.coupon_coupon_users',
                   'public.customer_customeraddress',
                   'public.customer_favouriterestaurant',
                   'public.geo_city', 'public.geo_country',
                   'public.geo_deliverypolygon', 'public.geo_district',
                   'public.geo_district_zipcodes',
                   'public.geo_street', 'public.geo_zipcode',
                   'public.newsletter_newslettersubscription',
                   'public.offers_offeravailability',
                   'public.offers_restaurantoffer',
                   'public.restaurant_additionalingredient',
                   'public.restaurant_additionalingredientset',
                   'public.restaurant_additionalingredientset_menu_items',
                   'public.restaurant_menuitem',
                   'public.restaurant_menuitemtag',
                   'public.restaurant_menusection',
                   'public.restaurant_menusectionavailability',
                   'public.restaurant_menusectioncategory',
                   'public.restaurant_promotedrestaurantslot',
                   'public.restaurant_restaurant',
                   'public.restaurant_restaurantaddress',
                   'public.restaurant_restaurantavailability',
                   'public.restaurant_restaurantcategory',
                   'public.restaurant_restaurantcategorygroup',
                   'public.restaurant_restaurantcategorygrouprelation',
                   'public.restaurant_restaurantcategoryrelation',
                   'public.restaurant_restaurantchain',
                   'public.restaurant_restaurantexternalreference',
                   'public.restaurant_restaurantreachability',
                   'public.restaurant_restauranttag',
                   'public.restaurant_taggedmenuitem',
                   'public.restaurant_taggedrestaurant',
                   'public.restaurant_targetdistrict',
                   'public.shared_emailcontact',
                   'public.shared_phonecontact', 'public.urlhandler_nurl',
                   'public.customer_customer', 'public.cart_orderitem',
                   'public.cart_order', 'public.payback_paybackcard',
                   'public.payback_paybackcardcustomer',
                   'public.payback_paybackcardorder',
                   'public.payback_paybacktransaction'
                   ],

        'bgk_de': ['getbk.Areas', 'getbk.campaign', 'getbk.Cities',
                   'getbk.company', 'getbk.Configuration', 'getbk.Cuisines',
                   'getbk.customeraddress', 'getbk.Customers',
                   'getbk.deliveryprovider', 'getbk.Discounts',
                   'getbk.Discountsattributions', 'getbk.Menucategories',
                   'getbk.Menus', 'getbk.MenusProducts',
                   'getbk.naomi_orders_dispatch', 'getbk.Newsletterusers',
                   'getbk.OrderAssignmentFlows',
                   'getbk.order_billing_information',
                   'getbk.Orderchoices', 'getbk.Orderdeclinereasons',
                   'getbk.OrderDeliveryproviderFlow',
                   'getbk.order_delivery_time',
                   'getbk.order_integration_data', 'getbk.OrderIssues',
                   'getbk.order_pickup_time', 'getbk.Orderproducts',
                   'getbk.Orders', 'getbk.Ordertoppings',
                   'getbk.order_vendor_delivery_code', 'getbk.Paymenttypes',
                   'getbk.platform', 'getbk.Products',
                   'getbk.Productvariations', 'getbk.Schedules',
                   'getbk.Specialdays', 'getbk.Status', 'getbk.Statusflows',
                   'getbk.Vendorcontacts', 'getbk.Vendordeliveries',
                   'getbk.VendordeliveriesAreas',
                   'getbk.VendordeliveriesPolygons',
                   'getbk.vendor_delivery_code', 'getbk.VendorDeliveryTime',
                   'getbk.Vendorflows', 'getbk.VendorPickupLocations',
                   'getbk.VendorReviews', 'getbk.Vendors',
                   'getbk.Vendorschains', 'getbk.VendorsCuisines',
                   'getbk.VendorsDiscounts', 'getbk.VendorsPaymenttypes',
                   'getbk.VendorStatus', 'getbk.Voucherattributions',
                   'getbk.Vouchers', 'getbk.voucher_schedule'],

        'lh_audit_de': ['audit.customer_changelog',
                        'audit.order_status_history'],
        'pde_audit_de': ['audit.customer_changelog',
                         'audit.order_status_history'],
        'lh_click_to_claim_de': ['public.campaign', 'public.voucher'],
        'pde_joker_de': ['public.action_logs',
                         'public.offer_requested_restaurants',
                         'public.offer_statuses', 'public.offers',
                         'public.promotion_definitions', 'public.promotions',
                         'public.reservations', 'public.restaurants',
                         'public.user_orders', 'public.users'],
        'midas': ['midas_data.bookings', 'midas_data.country_brands',
                  'midas_data.packages', 'midas_data.postcodes',
                  'midas_data.restaurants', 'midas_data.settings'],
        '9c': ['public.contract_plan', 'public.country', 'public.delivery',
               'public.delivery_address', 'public.delivery_platform',
               'public.driver_location_log', 'public.operator',
               'public.operator_contract_type', 'public.restaurant',
               'public.sms_notification'],
        'fd_de': ['production_de.accounting',
                  'production_de.accounting_vat_groups',
                  'production_de.ActiongroupsRoles', 'production_de.Areas',
                  'production_de.calculation_configuration_template',
                  'production_de.calls', 'production_de.campaign',
                  'production_de.campaign_vendor',
                  'production_de.chain_menu_group',
                  'production_de.characteristics_categories_translations',
                  'production_de.ChoicetemplateProducts',
                  'production_de.Choicetemplates', 'production_de.Cities',
                  'production_de.Cms', 'production_de.Communicationqueues',
                  'production_de.company', 'production_de.Configuration',
                  'production_de.Cuisines', 'production_de.customeraddress',
                  'production_de.Customers',
                  'production_de.deliveryprovider',
                  'production_de.desktopapp_order_lock',
                  'production_de.Discounts',
                  'production_de.Discountsattributions',
                  'production_de.event', 'production_de.event_action',
                  'production_de.event_action_message',
                  'production_de.event_polygon',
                  'production_de.flood_feature_event_log',
                  'production_de.Foodcaracteristics',
                  'production_de.foodpanda',
                  'production_de.fraud_validation_transaction',
                  'production_de.Globalcuisines',
                  'production_de.GprsPrinterRecord',
                  'production_de.Languages',
                  'production_de.Loyaltyprogramnames',
                  'production_de.Loyaltyprograms',
                  'production_de.master_categories',
                  'production_de.Menucategories',
                  'production_de.Menus',
                  'production_de.MenusProducts',
                  'production_de.messages_queue',
                  'production_de.Newsletterusers',
                  'production_de.option_value',
                  'production_de.OrderAssignmentFlows',
                  'production_de.Orderdeclinereasons',
                  'production_de.Orderproducts',
                  'production_de.Orders', 'production_de.Ordertoppings',
                  'production_de.payment_transaction',
                  'production_de.Paymenttypes',
                  'production_de.Products',
                  'production_de.products_nutrition_information',
                  'production_de.Productvariations',
                  'production_de.ProductvariationsChoicetemplates',
                  'production_de.ProductvariationsToppingtemplates',
                  'production_de.Roles', 'production_de.Schedules',
                  'production_de.Specialdays',
                  'production_de.Status',
                  'production_de.ToppingtemplateProducts',
                  'production_de.Toppingtemplates',
                  'production_de.Translations',
                  'production_de.TranslationStatus',
                  'production_de.Urlkeys', 'production_de.Users',
                  'production_de.UsersRoles',
                  'production_de.vendor_configuration',
                  'production_de.Vendorcontacts',
                  'production_de.Vendordeliveries',
                  'production_de.VendordeliveriesAreas',
                  'production_de.VendordeliveriesPolygons',
                  'production_de.VendorDeliveryTime',
                  'production_de.Vendorflows', 'production_de.VendorReviews',
                  'production_de.Vendors',
                  'production_de.vendors_additional_info',
                  'production_de.Vendorschains',
                  'production_de.VendorsCuisines',
                  'production_de.VendorsDiscounts',
                  'production_de.VendorsFoodcaracteristics',
                  'production_de.VendorsPaymenttypes',
                  'production_de.VendorsUsers',
                  'production_de.VendorsVouchers',
                  'production_de.VendorTags',
                  'production_de.Vouchers', 'production_de.Whitelabel',
                  'production_de.Voucherattributions',
                  'production_de.vendor_deliveries_polygon_adjustments_log',
                  'production_de.calculation_configuration',
                  'production_de.Statusflows'],
        'lh_payment_de': ['public.alembic_version', 'public.customer',
                          'public.notification',
                          'public.order_payment_record',
                          'public.payment_transaction',
                          'public.subscription'],
        'pde_payment_de': ['public.alembic_version', 'public.customer',
                           'public.notification',
                           'public.order_payment_record',
                           'public.payment_transaction',
                           'public.subscription'],
        'blacklisted': ['production_de.vendor_deliveries_'
                        'polygon_adjustments_log',
                        'production_de.calculation_configuration',
                        'production_de.Statusflows',
                        'public.alembic_version', 'public.customer',
                        'public.order_payment_record', 'public.subscription']
    }


# General utils function
def get_list_count_size(bucket, prefix):
    count, size = 0, 0
    for b in bucket.list_blobs(prefix=prefix):
        count += 1
        size += b.size
    return count, size


def get_blob_size(bucket, name):
    name_clean = name.replace('gs://', '')
    return storage.Blob(name_clean, bucket).size


def get_count_size_from_prefixes(bucket, prefixes):
    count, size = 0, 0
    existed_file_list = list()
    for p in prefixes:
        for b in bucket.list_blobs(prefix=p):
            existed_file_list.append('gs://dhg-backend/' + b.name)
            count += 1
            size += b.size
    return count, size, existed_file_list


def check_path_in_list(path, file_list):
    """
    Check if a file is in a list
    :param path: file needs to be checked
    :param file_list: list of files
    :return:
    """
    for f in file_list:
        if path in f:
            return True
    return False


def get_identity_column(source_name, table):
    """
    Get identity column of a specific table
    :param source_name: name of brand
    :param table: name of table
    :return:
    """
    identity_column = 'id'
    if source_name == 'pde_de' or source_name == 'lh_de':
        if table == 'public.backend_crmdata':
            identity_column = 'restaurant_id'
        elif table == 'public.cart_thirdpartyorderinfo':
            identity_column = 'order_id'

    elif source_name == 'bgk_de':
        if table == 'getbk.VendorStatus':
            identity_column = 'vendor_id'

    elif source_name == 'pde_joker_de':
        identity_dict = {'public.action_logs': 'action_log_id',
                         'public.offer_requested_restaurants': 'id',
                         'public.offer_statuses': 'status_id',
                         'public.offers': 'offer_id',
                         'public.promotion_definitions':
                             'promotion_definition_id',
                         'public.promotions': 'promotion_id',
                         'public.reservations': 'reservation_id',
                         'public.restaurants': 'restaurant_id',
                         'public.user_orders': 'order_id',
                         'public.users': 'user_id'}
        identity_column = identity_dict[table]

    elif source_name == '9c':
        identity_dict = {'public.contract_plan': 'id',
                         'public.country': 'id',
                         'public.delivery': 'delivery_timestamp',
                         'public.delivery_address': 'modified',
                         'public.delivery_platform': 'id',
                         'public.driver_location_log': 'upload_timestamp',
                         'public.operator': 'id',
                         'public.operator_contract_type': 'id',
                         'public.restaurant': 'id',
                         'public.restaurant_extension': 'belongs_to',
                         'public.sms_notification': 'id'}
        identity_column = identity_dict[table]

    elif source_name == 'fd_de' and table == \
            'production_de.products_nutrition_information':
        identity_column = 'product_id'

    return identity_column


def is_table_changed(source_name):
    """
    Check if there is a changelog for today
    :param source_name: name of brand
    :return: bool
    """
    source_prefix = source_name.split('_de')[0]
    today = datetime.now().strftime('%Y-%m-%d')

    storage_client = storage.Client()
    bucket = storage_client.bucket('dhg-backend')
    id_md5_prefix = 'dwh_psql_' + source_prefix + '/changelogs/'
    for bkt in bucket.list_blobs(prefix=id_md5_prefix):
        if today + '.changed.txt' in bkt.name:
            return True
    return False


# ULM jobs
def _get_ulm_input_size(task_type=None, date=None):
    """
    Return (INPUT__FILES_COUNT, INPUT_FILES_SIZE) for the specified ULM job
    :param task_type:
    :param date:
    :return:
    """
    return 0, 0


# CSV jobs
BUCKET_PREFIX = 'gs://dhg-backend/dwh_psql_'


def _get_csv_input_size(task_type, backend, date, day_diff=0):
    if task_type == 'find':
        return _get_csv_find_input_size(backend, date, day_diff)
    elif task_type == 'update':
        return _get_csv_update_input_size(backend, date, day_diff)
    elif task_type == 'recreate':
        return _get_csv_recreate_input_size(backend, date, day_diff)
    else:
        raise ValueError('Invalid CSV task type')


def _get_csv_find_input_size(backend, date=None, day_diff=0):
    count, size = 0, 0

    # Understand input files and compute metrics
    table_list = get_table_list[backend]
    blacklisted_list = get_table_list['blacklisted']
    source_splitted = backend.split('_de')
    today = date.strftime('%Y-%m-%d')
    yesterday = (date - datetime.timedelta(days=1 + day_diff)) \
        .strftime('%Y-%m-%d')

    today_path = 'gs://dhg-backend/dwh_psql_' + source_splitted[0] \
                 + '/id_md5_changes/' + today
    yesterday_path = 'gs://dhg-backend/dwh_psql_' + source_splitted[0] \
                     + '/unique/' + yesterday

    existed_file_list = []
    storage_client = storage.Client()
    bucket = storage_client.bucket('dhg-backend')

    id_md5_changes_prefix = 'dwh_psql_' + source_splitted[0] \
                            + '/id_md5_changes/' + today

    tmpCount, tmpSize = get_list_count_size(bucket, id_md5_changes_prefix)
    count += tmpCount
    size += tmpSize

    for table in table_list:
        if backend == 'fd_de' and table in blacklisted_list:
            continue

        if backend == 'lh_payment_de' and table in blacklisted_list:
            continue

        if backend == 'pde_payment_de' and table in blacklisted_list:
            continue

        output_mod = today_path + '/' + table + '/mod'

        if check_path_in_list(output_mod, existed_file_list):
            if check_path_in_list(output_mod + '/_SUCCESS', existed_file_list):
                continue

        count += 1
        size += get_blob_size(bucket, today_path + '/' + table + ".id_md5.csv")

        for b in bucket.list_blobs(
                prefix=yesterday_path + '/' + table):
            if 'csv' in b.name.split('.')[-1]:
                count += 1
                size += b.size

    return count, size


def _get_csv_update_input_size(backend, date=None, day_diff=0):
    count, size = 0, 0

    # Understand input files and compute metrics
    table_list = get_table_list[backend]
    blacklisted_list = get_table_list['blacklisted']
    source_splitted = backend.split('_de')
    today = date.strftime('%Y-%m-%d')
    yesterday = (date - datetime.timedelta(days=1 + day_diff)) \
        .strftime('%Y-%m-%d')

    full_file_base_path = 'gs://dhg-backend/dwh_psql_' + source_splitted[0] \
                          + '/updated/' + yesterday
    base_output_path = 'gs://dhg-backend/dwh_psql_' + source_splitted[0] \
                       + '/updated/' + today

    changed_file_base_path = 'gs://dhg-backend/dwh_psql_' \
                             + source_splitted[0] \
                             + '/id_md5_changes/' + today

    id_md5_changes_prefix = 'dwh_psql_' + source_splitted[0] \
                            + '/id_md5_changes/' + today
    unique_prefix = 'dwh_psql_' + source_splitted[0] + '/unique/' + today
    unique_full_prefix = 'dwh_psql_' + source_splitted[0] + '/unique_full/' \
                         + today
    updated_prefix = 'dwh_psql_' + source_splitted[0] + '/updated/' + today
    single_prefix = 'dwh_psql_' + source_splitted[0] + '/single/' + today

    storage_client = storage.Client()
    bucket = storage_client.bucket('dhg-backend')

    prefixes = [
        id_md5_changes_prefix, unique_prefix, unique_full_prefix,
        updated_prefix, single_prefix
    ]
    tmpCount, tmpSize, existed_file_list = \
        get_count_size_from_prefixes(bucket, prefixes)
    size += tmpSize
    count += tmpCount

    for table in table_list:
        continue_cond = (backend == 'fd_de' and table in blacklisted_list) \
                        or (backend == 'lh_payment_de'
                            and table in blacklisted_list)\
                        or (backend == 'pde_payment_de'
                            and table in blacklisted_list)
        if continue_cond:
            continue

        output_path = base_output_path + '/' + table
        output_path_succeeded = output_path + '/' + '_SUCCESS'

        if check_path_in_list(output_path + '/', existed_file_list):
            if check_path_in_list(output_path_succeeded, existed_file_list):
                continue

        for b in bucket.list_blobs(prefix=full_file_base_path + '/' + table):
            if 'csv' in b.name.split('.')[-1]:
                count += 1
                size += b.size

        mod_path = changed_file_base_path + '/' + table + ".mod.csv"
        new_path = changed_file_base_path + '/' + table + ".new.csv"
        del_path = changed_file_base_path + '/' + table + "/del/*.csv"

        check_del = del_path[:-5] + 'part-0000'
        if check_path_in_list(check_del, existed_file_list):
            for b in bucket.list_blobs(
                    prefix=changed_file_base_path + '/' + table + "/del"):
                if 'csv' in b.name.split('.')[-1]:
                    count += 1
                    size += b.size

        if mod_path in existed_file_list:
            count += 1
            size += get_blob_size(bucket, mod_path)

        if new_path in existed_file_list:
            count += 1
            size += get_blob_size(bucket, new_path)

    return count, size


def _get_csv_recreate_input_size(backend, date, day_diff=0):
    count, size = 0, 0

    # Understand input files and compute metrics
    if not is_table_changed(backend):
        return count, size

    source_prefix = backend.split('_de')[0]
    today = date.strftime('%Y-%m-%d')
    yesterday = (date - datetime.timedelta(days=1 + day_diff)).strftime(
        '%Y-%m-%d')

    storage_client = storage.Client()
    bucket = storage_client.bucket('dhg-backend')

    changed_path = BUCKET_PREFIX + source_prefix + '/changelogs/' + \
        today + '.changed.txt'
    name_clean = changed_path.replace('gs://', '')
    changed_tables = storage.Blob(name_clean, bucket).download_as_string()

    updated_prefix = 'dwh_psql_' + source_prefix + '/updated/' + yesterday
    unique_prefix = 'dwh_psql_' + source_prefix + '/unique/' + yesterday

    prefixes = [
        updated_prefix, unique_prefix
    ]
    tmpCount, tmpSize, existed_file_list = \
        get_count_size_from_prefixes(bucket, prefixes)
    size += tmpSize
    count += tmpCount

    for table in changed_tables:
        table = table.split('\n')[0]
        input = BUCKET_PREFIX + source_prefix + '/csv_export_md5'
        unique_output_path = BUCKET_PREFIX + source_prefix + '/unique/' \
            + yesterday

        if check_path_in_list(unique_output_path + '/' + table + '/',
                              existed_file_list):
            if check_path_in_list(unique_output_path + '/' + table
                                  + '/_SUCCESS',
                                  existed_file_list):
                continue

        count += 1
        size += get_blob_size(bucket, input + '/' + table + ".csv")

    # Return results
    return count, size


# Generic function
def get_input_size(job_type, task_type=None, backend=None,
                   date=None, day_diff=0):
    """
    Returns a tuple of the form (INPUT_FILES_COUNT, INPUT_FILES_SIZE) for the
    given job, performing the specified task on the passed backend
    :param day_diff:
    :param date:
    :param job_type:
    :param task_type:
    :param backend:
    :return:
    """
    if job_type == 'csv':
        return _get_csv_input_size(task_type, backend, date, day_diff)
    elif job_type == 'ulm':
        return _get_ulm_input_size(task_type, date)
    else:
        raise ValueError('Invalid job type')
