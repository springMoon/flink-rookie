# 库存场景计算

## 源表

| 表名 | 内容 |
| --- | --- |
| ods_poc_dim_product_doc_api |	dim_产品档案 |
| ods_poc_dim_agency_doc_api |	dim_经销商档案 |
| ods_poc_dim_dpt_doc_api	| dim_部门档案 |
| ods_poc_k3_customer	| k3_客户档案 |
| ods_poc_k3_material	| k3_物料档案 |
| ods_poc_k3_sal_outstock	| k3_销售出库单主表 |
| ods_poc_k3_sal_outstockentry	| k3_销售出库单子表 |
| ods_poc_sfa_in_out	| sfa_出入库类型 | 
| ods_poc_sfa_stocking	| sfa_期初库存 | 
| ods_poc_sfa_distributoroutstocklist	| sfa_经销商进出库扫码主表 | 
| ods_poc_sfa_distributoroutstocklist_detail	| sfa_经销商进出库扫码子表 |
| ods_poc_sfa_distributor_department	| sfa_经销商部门对照关系 |



## 动销
业务逻辑： 非核心产品采购 + 核心产品销售 C002/C004

