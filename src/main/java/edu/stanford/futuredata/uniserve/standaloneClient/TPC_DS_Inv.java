package edu.stanford.futuredata.uniserve.standaloneClient;

import java.util.List;

public class TPC_DS_Inv {
    public static final Integer numberOfTables = 24;

    public static final int storeSalesSize = 240485;
    public static final int storeReturnsSize = 23925;
    public static final int catalogSalesSize = 89807;
    public static final int catalogReturnsSize = 8923;
    public static final int webSalesSize = 47726;
    public static final int webReturnsSize = 4714;
    public static final int inventorySize = 261261;
    public static final int storeSize = 2;
    public static final int callCenterSize = 2;
    public static final int catalogPageSize = 11718;
    public static final int webSiteSize = 2;
    public static final int webPageSize = 2;
    public static final int warehouseSize = 1;
    public static final int customerSize = 6000;
    public static final int customerAddressSize = 3000;
    public static final int customerDemographicsSize = 1920800;
    public static final int dateDimSize = 73049;
    public static final int householdDemographicsSize = 7200;
    public static final int itemSize = 2000;
    public static final int incomeBandSize = 20;
    public static final int promotionSize = 18;
    public static final int reasonSize = 2;
    public static final int shipModeSize = 20;
    public static final int timeDimSize = 86400;

    public static final int BstoreSalesSize = 1441375;
    public static final int BstoreReturnsSize = 143733;
    public static final int BcatalogSalesSize = 720352;
    public static final int BcatalogReturnsSize = 71974;
    public static final int BwebSalesSize = 359584;
    public static final int BwebReturnsSize = 35716;
    public static final int BinventorySize = 2088522;
    public static final int BstoreSize = 6;
    public static final int BcallCenterSize = 2;
    public static final int BcatalogPageSize = 11718;
    public static final int BwebSiteSize = 14;
    public static final int BwebPageSize = 30;
    public static final int BwarehouseSize = 2;
    public static final int BcustomerSize = 50000;
    public static final int BcustomerAddressSize = 25000;
    public static final int BcustomerDemographicsSize = 1920800;
    public static final int BdateDimSize = 73049;
    public static final int BhouseholdDemographicsSize = 7200;
    public static final int BitemSize = 8000;
    public static final int BincomeBandSize = 20;
    public static final int BpromotionSize = 150;
    public static final int BreasonSize = 17;
    public static final int BshipModeSize = 20;
    public static final int BtimeDimSize = 86400;

    public static final List<Integer> Bsizes = List.of(
            BstoreSalesSize,
            BstoreReturnsSize,
            BcatalogSalesSize,
            BcatalogReturnsSize,
            BwebSalesSize,
            BwebReturnsSize,
            BinventorySize,
            BstoreSize,
            BcallCenterSize,
            BcatalogPageSize,
            BwebSiteSize,
            BwebPageSize,
            BwarehouseSize,
            BcustomerSize,
            BcustomerAddressSize,
            BcustomerDemographicsSize,
            BdateDimSize,
            BhouseholdDemographicsSize,
            BitemSize,
            BincomeBandSize,
            BpromotionSize,
            BreasonSize,
            BshipModeSize,
            BtimeDimSize
    );

    public static final List<Integer> sizes = List.of(
            storeSalesSize,
            storeReturnsSize,
            catalogSalesSize,
            catalogReturnsSize,
            webSalesSize,
            webReturnsSize,
            inventorySize,
            storeSize,
            callCenterSize,
            catalogPageSize,
            webSiteSize,
            webPageSize,
            warehouseSize,
            customerSize,
            customerAddressSize,
            customerDemographicsSize,
            dateDimSize,
            householdDemographicsSize,
            itemSize,
            incomeBandSize,
            promotionSize,
            reasonSize,
            shipModeSize,
            timeDimSize
    );


    public final static String callCenterPath = "/call_center.dat";
    public final static String catalogPagePath = "/catalog_page.dat";
    public final static String catalogReturnsPath = "/catalog_returns.dat";
    public final static String catalogSalesPath = "/catalog_sales.dat";
    public final static String customerPath = "/customer.dat";
    public final static String customerAddressPath = "/customer_address.dat";
    public final static String customerDemographicsPath = "/customer_demographics.dat";
    public final static String dateDimPath = "/date_dim.dat";
    public final static String householdDemographicsPath = "/household_demographics.dat";
    public final static String incomeBandPath = "/income_band.dat";
    public final static String inventoryPath = "/inventory.dat";
    public final static String itemPath = "/item.dat";
    public final static String promotionPath = "/promotion.dat";
    public final static String reasonPath = "/reason.dat";
    public final static String shipModePath = "/ship_mode.dat";
    public final static String storePath = "/store.dat";
    public final static String storeReturnsPath = "/store_returns.dat";
    public final static String storeSalesPath = "/store_sales.dat";
    public final static String timeDimPath = "/time_dim.dat";
    public final static String warehousePath = "/warehouse.dat";
    public final static String webPagePath = "/web_page.dat";
    public final static String webReturnsPath = "/web_returns.dat";
    public final static String webSalesPath = "/web_sales.dat";
    public final static String webSitePath = "/web_site.dat";

    public static final List<String> Store_sales_key = List.of("ss_item_sk", "ss_ticket_number");
    public static final List<String> Store_returns_key = List.of("sr_item_sk", "sr_ticket_number");
    public static final List<String> Catalog_sales_key = List.of("cs_item_sk", "cs_order_number");
    public static final List<String> Catalog_returns_key = List.of("cr_item_sk", "cr_order_number");
    public static final List<String> Web_sales_key = List.of("ws_item_sk", "ws_order_number");
    public static final List<String> Web_returns_key = List.of("wr_item_sk", "wr_order_number");
    public static final List<String> Inventory_key = List.of("inv_date_sk", "inv_item_sk", "inv_warehouse_sk");
    public static final List<String> Store_key = List.of("s_store_sk");
    public static final List<String> Call_center_key = List.of("cc_call_center_sk");
    public static final List<String> Catalog_page_key = List.of("cp_catalog_page_sk");
    public static final List<String> Web_site_key = List.of("web_site_sk");
    public static final List<String> Web_page_key = List.of("wp_web_page_sk");
    public static final List<String> Warehouse_key = List.of("w_warehouse_sk");
    public static final List<String> Customer_key = List.of("c_customer_sk");
    public static final List<String> Customer_address_key = List.of("ca_address_sk");
    public static final List<String> Customer_demographics_key = List.of("cd_demo_sk");
    public static final List<String> Date_dim_key = List.of("d_date_sk");
    public static final List<String> Household_demographics_key = List.of("hd_demo_sk");
    public static final List<String> Item_key = List.of("i_item_sk");
    public static final List<String> Income_band_key = List.of("ib_income_band_sk");
    public static final List<String> Promotion_key = List.of("p_promo_sk");
    public static final List<String> Reason_key = List.of("r_reason_sk");
    public static final List<String> Ship_mode_key = List.of("sm_ship_mode_sk");
    public static final List<String> Time_dim_key = List.of("t_time_sk");

    public static final List<String> Store_sales_schema = List.of(
            "ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk",
            "ss_customer_sk", "ss_cdemo_sk", "ss_hdemo_sk",
            "ss_addr_sk", "ss_store_sk", "ss_promo_sk",
            "ss_ticket_number", "ss_quantity", "ss_wholesale_cost",
            "ss_list_price", "ss_sales_price", "ss_ext_discount_amt",
            "ss_ext_sales_price", "ss_ext_wholesale_cost", "ss_ext_list_price",
            "ss_ext_tax", "ss_coupon_amt", "ss_net_paid",
            "ss_net_paid_inc_tax", "ss_net_profit"
    );
    public static final List<String> Store_returns_schema = List.of(
            "sr_returned_date_sk", "sr_returned_time_sk", "sr_item_sk",
            "sr_customer_sk", "sr_cdemo_sk", "sr_hdemo_sk",
            "sr_addr_sk", "sr_store_sk", "sr_reason_sk",
            "sr_ticket_number", "sr_return_quantity", "sr_return_amt",
            "sr_return_tax", "sr_return_amt_income_tax", "sr_fee",
            "sr_return_ship_cost", "sr_refunded_cash", "sr_reversed_charge",
            "sr_store_credit", "sr_net_loss"
    );
    public static final List<String> Catalog_sales_schema = List.of(
            "cs_sold_date_sk", "cs_sold_time_sk", "cs_ship_date_sk",
            "cs_bill_customer_sk", "cs_bill_cdemo_sk", "cs_bill_hdemo_sk",
            "cs_bill_addr_sk",
            "cs_ship_customer_sk",
            "cs_ship_cdemo_sk",
            "cs_ship_hdemo_sk",
            "cs_ship_addr_sk",
            "cs_call_center_sk",
            "cs_catalog_page_sk",
            "cs_ship_mode_sk",
            "cs_warehouse_sk",
            "cs_item_sk",
            "cs_promo_sk",
            "cs_order_number",
            "cs_quantity",
            "cs_wholesale_cost",
            "cs_list_price",
            "cs_sales_price",
            "cs_ext_discount_amt",
            "cs_ext_sales_price",
            "cs_ext_wholesale_cost",
            "cs_ext_list_price",
            "cs_ext_tax",
            "cs_coupon_amt",
            "cs_ext_ship_cost",
            "cs_net_paid",
            "cs_net_paid_inc_tax",
            "cs_net_paid_inc_ship",
            "cs_net_paid_inc_ship_tax",
            "cs_net_profit"
    );
    public static final List<String> Catalog_returns_schema  = List.of(
            "cr_returned_date_sk",
            "cr_returned_time_sk",
            "cr_item_sk",
            "cr_refunded_customer_sk",
            "cr_refunded_cdemo_sk",
            "cr_refunded_hdemo_sk",
            "cr_refunded_addr_sk",
            "cr_returning_customer_sk",
            "cr_returning_cdemo_sk",
            "cr_returning_hdemo_sk",
            "cr_returning_addr_sk",
            "cr_call_center_sk",
            "cr_catalog_page_sk",
            "cr_ship_mode_sk",
            "cr_warehouse_sk",
            "cr_reason_sk",
            "cr_order_number",
            "cr_return_quantity",
            "cr_return_amount",
            "cr_return_tax",
            "cr_return_amt_inc_tax",
            "cr_fee",
            "cr_return_ship_cost",
            "cr_refunded_cash",
            "cr_reversed_charge",
            "cr_store_credit",
            "cr_net_loss"
    );
    public static final List<String> Web_sales_schema  = List.of(
            "ws_sold_date_sk",
            "ws_sold_time_sk",
            "ws_ship_date_sk",
            "ws_item_sk",
            "ws_bill_customer_sk",
            "ws_bill_cdemo_sk",
            "ws_bill_hdemo_sk",
            "ws_bill_addr_sk",
            "ws_ship_customer_sk",
            "ws_ship_cdemo_sk",
            "ws_ship_hdemo_sk",
            "ws_ship_addr_sk",
            "ws_web_page_sk",
            "ws_web_site_sk",
            "ws_ship_mode_sk",
            "ws_warehouse_sk",
            "ws_promo_sk",
            "ws_order_number",
            "ws_quantity",
            "ws_wholesale_cost",
            "ws_list_price",
            "ws_sales_price",
            "ws_ext_discount_amt",
            "ws_ext_sales_price",
            "ws_ext_wholesale_cost",
            "ws_ext_list_price",
            "ws_ext_tax",
            "ws_coupon_amt",
            "ws_ext_ship_cost",
            "ws_net_paid",
            "ws_net_paid_inc_tax",
            "ws_net_paid_inc_ship",
            "ws_net_paid_inc_ship_tax",
            "ws_net_profit"
    );
    public static final List<String> Web_returns_schema  = List.of(
            "wr_returned_date_sk",
            "wr_returned_time_sk",
            "wr_item_sk",
            "wr_refunded_customer_sk",
            "wr_refunded_cdemo_sk",
            "wr_refunded_hdemo_sk",
            "wr_refunded_addr_sk",
            "wr_returning_customer_sk",
            "wr_returning_cdemo_sk",
            "wr_returning_hdemo_sk",
            "wr_returning_addr_sk",
            "wr_web_page_sk",
            "wr_reason_sk",
            "wr_order_number",
            "wr_return_quantity",
            "wr_return_amt",
            "wr_return_tax",
            "wr_return_amt_inc_tax",
            "wr_fee",
            "wr_return_ship_cost",
            "wr_refunded_cash",
            "wr_reversed_charge",
            "wr_account_credit",
            "wr_net_loss"
    );
    public static final List<String> Inventory_schema  = List.of(
            "inv_date_sk",
            "inv_item_sk",
            "inv_warehouse_sk",
            "inv_quantity_on_hand"
    );
    public static final List<String> Store_schema  = List.of(
            "s_store_sk",
            "s_store_id",
            "s_rec_start_date",
            "s_rec_end_date",
            "s_closed_date_sk",
            "s_store_name",
            "s_number_employees",
            "s_floor_space",
            "s_hours",
            "s_manager",
            "s_market_id",
            "s_geography_class",
            "s_market_desc",
            "s_market_manager",
            "s_division_id",
            "s_division_name",
            "s_company_id",
            "s_company_name",
            "s_street_number",
            "s_street_name",
            "s_street_type",
            "s_suite_number",
            "s_city",
            "s_county",
            "s_state",
            "s_zip",
            "s_country",
            "s_gmt_offset",
            "s_tax_percentage"
    );
    public static final List<String> Call_center_schema  = List.of(
            "cc_call_center_sk",
            "cc_call_center_id",
            "cc_rec_start_date",
            "cc_rec_end_date",
            "cc_closed_date_sk",
            "cc_open_date_sk",
            "cc_name",
            "cc_class",
            "cc_employees",
            "cc_sq_ft",
            "cc_hours",
            "cc_manager",
            "cc_mkt_id",
            "cc_mkt_class",
            "cc_mkt_desc",
            "cc_market_manager",
            "cc_division",
            "cc_division_name",
            "cc_company",
            "cc_company_name",
            "cc_street_number",
            "cc_street_name",
            "cc_street_type",
            "cc_suite_number",
            "cc_city",
            "cc_county",
            "cc_state",
            "cc_zip",
            "cc_country",
            "cc_gmt_offset",
            "cc_tax_percentage"
    );
    public static final List<String> Catalog_page_schema  = List.of(
            "cp_catalog_page_sk",
            "cp_catalog_page_id",
            "cp_start_date_sk",
            "cp_end_date_sk",
            "cp_department",
            "cp_catalog_number",
            "cp_catalog_page_number",
            "cp_description",
            "cp_type"
    );
    public static final List<String> Web_site_schema  = List.of(
            "web_site_sk",
            "web_site_id",
            "web_rec_start_date",
            "web_rec_end_date",
            "web_name",
            "web_open_date_sk",
            "web_close_date_sk",
            "web_class",
            "web_manager",
            "web_mkt_id",
            "web_mkt_class",
            "web_mkt_desc",
            "web_market_manager",
            "web_company_id",
            "web_company_name",
            "web_street_number",
            "web_street_name",
            "web_street_type",
            "web_suite_number",
            "web_city",
            "web_county",
            "web_state",
            "web_zip",
            "web_country",
            "web_gmt_offset",
            "web_tax_percentage"
    );
    public static final List<String> Web_page_schema  = List.of(
            "wp_web_page_sk",
            "wp_web_page_id",
            "wp_rec_start_date",
            "wp_rec_end_date",
            "wp_creation_date_sk",
            "wp_access_date_sk",
            "wp_autogen_flag",
            "wp_customer_sk",
            "wp_url",
            "wp_type",
            "wp_char_count",
            "wp_link_count",
            "wp_image_count",
            "wp_max_ad_count"
    );
    public static final List<String> Warehouse_schema  = List.of(
            "w_warehouse_sk",
            "w_warehouse_id",
            "w_warehouse_name",
            "w_warehouse_sq_ft",
            "w_street_number",
            "w_street_name",
            "w_street_type",
            "w_suite_number",
            "w_city",
            "w_county",
            "w_state",
            "w_zip",
            "w_country",
            "w_gmt_offset"
    );
    public static final List<String> Customer_schema  = List.of(
            "c_customer_sk",
            "c_customer_id",
            "c_current_cdemo_sk",
            "c_current_hdemo_sk",
            "c_current_addr_sk",
            "c_first_shipto_date_sk",
            "c_first_sales_date_sk",
            "c_salutation",
            "c_first_name",
            "c_last_name",
            "c_preferred_cust_flag",
            "c_birth_day",
            "c_birth_month",
            "c_birth_year",
            "c_birth_country",
            "c_login",
            "c_email_address",
            "c_last_review_date_sk"
    );
    public static final List<String> Customer_address_schema  = List.of(
            "ca_address_sk",
            "ca_address_id",
            "ca_street_number",
            "ca_street_name",
            "ca_street_type",
            "ca_suite_number",
            "ca_city",
            "ca_county",
            "ca_state",
            "ca_zip",
            "ca_country",
            "ca_gmt_offset",
            "ca_location_type"
    );
    public static final List<String> Customer_demographics_schema  = List.of(
            "cd_demo_sk",
            "cd_gender",
            "cd_marital_status",
            "cd_education_status",
            "cd_purchase_estimate",
            "cd_credit_rating",
            "cd_dep_count",
            "cd_dep_employed_count",
            "cd_dep_college_count"
    );
    public static final List<String> Date_dim_schema  = List.of(
            "d_date_sk",
            "d_date_id",
            "d_date",
            "d_month_seq",
            "d_week_seq",
            "d_quarter_seq",
            "d_year",
            "d_dow",
            "d_moy",
            "d_dom",
            "d_qoy",
            "d_fy_year",
            "d_fy_quarter_seq",
            "d_fy_week_seq",
            "d_day_name",
            "d_quarter_name",
            "d_holiday",
            "d_weekend",
            "d_following_holiday",
            "d_first_dom",
            "d_last_dom",
            "d_same_day_ly",
            "d_same_day_lq",
            "d_current_day",
            "d_current_week",
            "d_current_month",
            "d_current_quarter",
            "d_current_year"
    );
    public static final List<String> Household_demographics_schema  = List.of(
            "hd_demo_sk",
            "hd_income_band_sk",
            "hd_buy_potential",
            "hd_dep_count",
            "hd_vehicle_count"
    );
    public static final List<String> Item_schema  = List.of(
            "i_item_sk",
            "i_item_id",
            "i_rec_start_date",
            "i_rec_end_date",
            "i_item_desc",
            "i_current_price",
            "i_wholesale_cost",
            "i_brand_id",
            "i_brand",
            "i_class_id",
            "i_class",
            "i_category_id",
            "i_category",
            "i_manufact_id",
            "i_manufact",
            "i_size",
            "i_formulation",
            "i_color",
            "i_units",
            "i_container",
            "i_manager_id",
            "i_product_name"
    );
    public static final List<String> Income_band_schema  = List.of(
            "ib_income_band_sk",
            "ib_lower_bound",
            "ib_upper_bound"
    );
    public static final List<String> Promotion_schema  = List.of(
            "p_promo_sk",
            "p_promo_id",
            "p_start_date_sk",
            "p_end_date_sk",
            "p_item_sk",
            "p_cost",
            "p_response_target",
            "p_promo_name",
            "p_channel_dmail",
            "p_channel_email",
            "p_channel_catalog",
            "p_channel_tv",
            "p_channel_radio",
            "p_channel_press",
            "p_channel_event",
            "p_channel_demo",
            "p_channel_details",
            "p_purpose",
            "p_discount_active"
    );
    public static final List<String> Reason_schema  = List.of(
            "r_reason_sk",
            "r_reason_id",
            "r_reason_desc"
    );
    public static final List<String> Ship_mode_schema  = List.of(
            "sm_ship_mode_sk",
            "sm_ship_mode_id",
            "sm_type",
            "sm_code",
            "sm_carrier",
            "sm_contract"
    );
    public static final List<String> Time_dim_schema  = List.of(
            "t_time_sk",
            "t_time_id",
            "t_time",
            "t_hour",
            "t_minute",
            "t_second",
            "t_am_pm",
            "t_shift",
            "t_sub_shift",
            "t_meal_time"
    );
    public static final List<String> Dsdgen_version_schema = List.of(
            "dv_version",
            "dv_create_date",
            "dv_create_time",
            "dv_cmdline_args"
    );

    public static final Integer id__t = 0;
    public static final Integer string__t = 1;
    public static final Integer int__t = 2;
    public static final Integer dec__t = 3;
    public static final Integer date__t = 4;
    public static final List<Integer> store_sales_type = List.of(
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, int__t, dec__t,
            dec__t, dec__t, dec__t,
            dec__t, dec__t, dec__t,
            dec__t, dec__t, dec__t,
            dec__t, dec__t
    );
    public static final List<Integer> store_returns_type = List.of(
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, int__t, dec__t,
            dec__t, dec__t, dec__t,
            dec__t, dec__t, dec__t,
            dec__t, dec__t
    );
    public static final List<Integer> catalog_sales_type = List.of(
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            int__t, dec__t, dec__t,
            dec__t, dec__t, dec__t,
            dec__t, dec__t, dec__t,
            dec__t, dec__t, dec__t,
            dec__t, dec__t, dec__t,
            dec__t);
    public static final List<Integer> catalog_returns_type = List.of(
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, int__t,
            dec__t, dec__t, dec__t,
            dec__t, dec__t, dec__t,
            dec__t, dec__t, dec__t
    );
    public static final List<Integer> web_sales_type = List.of(
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            int__t, dec__t, dec__t,
            dec__t, dec__t, dec__t,
            dec__t, dec__t, dec__t,
            dec__t, dec__t, dec__t,
            dec__t, dec__t, dec__t,
            dec__t
    );
    public static final List<Integer> web_returns_type = List.of(
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, id__t,
            id__t, id__t, int__t,
            dec__t, dec__t, dec__t,
            dec__t, dec__t, dec__t,
            dec__t, dec__t, dec__t
    );
    public static final List<Integer> inventory_type = List.of(
            id__t, id__t, id__t,
            int__t
    );
    public static final List<Integer> store_type = List.of(
            id__t, string__t, date__t,
            date__t, id__t, string__t,
            int__t, int__t, string__t,
            string__t, int__t, string__t,
            string__t, string__t, int__t,
            string__t, int__t, string__t,
            string__t, string__t, string__t,
            string__t, string__t, string__t,
            string__t, string__t, string__t,
            dec__t, dec__t
    );
    public static final List<Integer> call_center_type = List.of(
            id__t, string__t, date__t,
            date__t, id__t, id__t,
            string__t, string__t, int__t,
            int__t, string__t, string__t,
            int__t, string__t, string__t,
            string__t, int__t, string__t,
            int__t, string__t, string__t,
            string__t, string__t, string__t,
            string__t, string__t, string__t,
            string__t, string__t, dec__t,
            dec__t
    );
    public static final List<Integer> catalog_page_type = List.of(
            id__t, string__t, id__t,
            id__t, string__t, int__t,
            int__t, string__t, string__t
    );
    public static final List<Integer> web_site_type = List.of(
            id__t, string__t, date__t,
            date__t, string__t, id__t,
            id__t, string__t, string__t,
            int__t, string__t, string__t,
            string__t, int__t, string__t,
            string__t, string__t, string__t,
            string__t, string__t, string__t,
            string__t, string__t, string__t,
            dec__t, dec__t
    );
    public static final List<Integer> web_page_type = List.of(
            id__t, string__t, date__t,
            date__t, id__t, id__t,
            string__t, id__t, string__t,
            string__t, int__t, int__t,
            int__t, int__t
    );
    public static final List<Integer> warehouse_type = List.of(
            id__t, string__t, string__t,
            int__t, string__t, string__t,
            string__t, string__t, string__t,
            string__t, string__t, string__t,
            string__t, dec__t
    );
    public static final List<Integer> customer_type = List.of(
            id__t, string__t, id__t,
            id__t, id__t, id__t,
            id__t, string__t, string__t,
            string__t, string__t, int__t,
            int__t, int__t, string__t,
            string__t, string__t, id__t
    );
    public static final List<Integer> customer_address_type = List.of(
            id__t, string__t, string__t,
            string__t,string__t,string__t,
            string__t,string__t,string__t,
            string__t, string__t, dec__t,
            string__t
    );
    public static final List<Integer> customer_demographics_type = List.of(
            id__t, string__t, string__t,
            string__t, int__t, string__t,
            int__t, int__t, int__t
    );
    public static final List<Integer> date_dim_type = List.of(
            id__t, string__t, date__t,
            int__t, int__t, int__t,
            int__t, int__t, int__t,
            int__t, int__t, int__t,
            int__t, int__t, string__t,
            string__t, string__t, string__t,
            string__t, int__t, int__t,
            int__t, int__t, string__t,
            string__t, string__t, string__t,
            string__t
    );
    public static final List<Integer> household_demographics_type = List.of(
            id__t, id__t, string__t,
            int__t, int__t
    );
    public static final List<Integer> item_type = List.of(
            id__t, string__t, date__t,
            date__t, string__t, dec__t,
            dec__t, int__t, string__t,
            int__t, string__t, int__t,
            string__t, int__t, string__t,
            string__t, string__t, string__t,
            string__t, string__t, int__t,
            string__t
    );
    public static final List<Integer> income_band_type = List.of(
            id__t, int__t, int__t
    );
    public static final List<Integer> promotion_type = List.of(
            id__t, string__t, id__t,
            id__t, id__t, dec__t,
            int__t, string__t, string__t,
            string__t, string__t, string__t,
            string__t, string__t, string__t,
            string__t, string__t, string__t,
            string__t
    );
    public static final List<Integer> reason_type = List.of(
            id__t, string__t, string__t
    );
    public static final List<Integer> ship_mode_type = List.of(
            id__t, string__t, string__t,
            string__t, string__t, string__t
    );
    public static final List<Integer> time_dim_type = List.of(
            id__t, string__t, int__t,
            int__t, int__t, int__t,
            string__t, string__t, string__t,
            string__t
    );






    public static final List<String> names = List.of(
            "store_sales",
            "store_returns",
            "catalog_sales",
            "catalog_returns",
            "web_sales",
            "web_returns",
            "inventory",
            "store",
            "call_center",
            "catalog_page",
            "web_site",
            "web_page",
            "warehouse",
            "customer",
            "customer_address",
            "customer_demographics",
            "date_dim",
            "household_demographics",
            "item",
            "income_band",
            "promotion",
            "reason",
            "ship_mode",
            "time_dim"
    );
    public static final List<String> paths = List.of(
            storeSalesPath,
            storeReturnsPath,
            catalogSalesPath,
            catalogReturnsPath,
            webSalesPath,
            webReturnsPath,
            inventoryPath,
            storePath,
            callCenterPath,
            catalogPagePath,
            webSitePath,
            webPagePath,
            warehousePath,
            customerPath,
            customerAddressPath,
            customerDemographicsPath,
            dateDimPath,
            householdDemographicsPath,
            itemPath,
            incomeBandPath,
            promotionPath,
            reasonPath,
            shipModePath,
            timeDimPath
    );
    public static final List<List<String>> schemas = List.of(
            Store_sales_schema,
            Store_returns_schema,
            Catalog_sales_schema,
            Catalog_returns_schema,
            Web_sales_schema,
            Web_returns_schema,
            Inventory_schema,
            Store_schema,
            Call_center_schema,
            Catalog_page_schema,
            Web_site_schema,
            Web_page_schema,
            Warehouse_schema,
            Customer_schema,
            Customer_address_schema,
            Customer_demographics_schema,
            Date_dim_schema,
            Household_demographics_schema,
            Item_schema,
            Income_band_schema,
            Promotion_schema,
            Reason_schema,
            Ship_mode_schema,
            Time_dim_schema
    );
    public static final List<List<String>> keys = List.of(
            Store_sales_key,
            Store_returns_key,
            Catalog_sales_key,
            Catalog_returns_key,
            Web_sales_key,
            Web_returns_key,
            Inventory_key,
            Store_key,
            Call_center_key,
            Catalog_page_key,
            Web_site_key,
            Web_page_key,
            Warehouse_key,
            Customer_key,
            Customer_address_key,
            Customer_demographics_key,
            Date_dim_key,
            Household_demographics_key,
            Item_key,
            Income_band_key,
            Promotion_key,
            Reason_key,
            Ship_mode_key,
            Time_dim_key
    );
    public static final List<List<Integer>> types = List.of(
            store_sales_type,
            store_returns_type,
            catalog_sales_type,
            catalog_returns_type,
            web_sales_type,
            web_returns_type,
            inventory_type,
            store_type,
            call_center_type,
            catalog_page_type,
            web_site_type,
            web_page_type,
            warehouse_type,
            customer_type,
            customer_address_type,
            customer_demographics_type,
            date_dim_type,
            household_demographics_type,
            item_type,
            income_band_type,
            promotion_type,
            reason_type,
            ship_mode_type,
            time_dim_type
    );
}