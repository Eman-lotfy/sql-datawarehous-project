CREATE VIEW gold.dim_products AS
SELECT 
       row_number() over( order by pn.prd_start_dt, pn.prd_key ) AS product_key,
       pn.prd_id AS product_id 
	  ,pn.prd_key AS product_number
      ,pn.prd_nm AS Product_name
      ,pn.cat_id AS category_id
      ,pc.cat AS category
	  ,pc.subcat AS subcategory
	  ,pc.maintenance
      ,pn.prd_cost AS cost
      ,pn.prd_line AS product_line
      ,pn.prd_start_dt AS start_date
  FROM DataWarehouse.Silver.crm_prd_info pn
  left join Silver.erp_px_cat_g1v2 pc
  on   pn.cat_id = pc.id
  where pn.prd_end_dt is null  -----filter out allhistorical data


CREATE VIEW gold.dim_customers  AS
SELECT 
      ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key
	  ,ci.cst_id AS customer_id 
      ,ci.cst_key AS customer_number
      ,ci.cst_firstname AS first_name
      ,ci.cst_lastname AS last_name
	  ,la.Cntry AS country
      ,ci.cst_material_status AS marital_status
      ,case when ci.cst_gnr != 'n/a' THEN ci.cst_gnr
	   ELSE COALESCE (ca.gen , 'n/a')
	   END AS gender
	   , ca.bdate As birthdate
      ,ci.cst_create_data AS create_date 
  FROM  DataWarehouse.Silver.crm_cust_info ci
  left join Silver.erp_cust_az12 ca
  on   ci.cst_key = ca.cid 
  left join Silver.erp_loc_a101 la
  on   ci.cst_key = la.cid

 Create view gold.fact_sales AS
SELECT 
      sls_ord_num  AS order_number
	  ,pr.product_key
	  ,cu.customer_key
      ,sls_order_dt AS order_date
      ,sls_ship_dt AS shipping_date
      ,sls_due_dt AS due_date
      ,sls_sales AS sales
      ,sls_quantity as quantity
      ,sls_Price   AS price  
  FROM Silver.crm_sales_details sd
  LEFT JOIN Gold.dim_products pr
  on sd.sls_prod_key = pr.product_number
  left join Gold.dim_customers cu
  on sd.sls_cust_id = cu.customer_id
