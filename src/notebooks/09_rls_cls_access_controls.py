# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Overview and Configuration
# MAGIC %md
# MAGIC # Street Safety Data Security Implementation
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook implements Unity Catalog **ABAC (Attribute-Based Access Control)** policies using governed tags to restrict access to sensitive street safety data based on the Dangerous Rating (0.1-0.9 scale).
# MAGIC
# MAGIC ## Critical Design Decision: Why a Governed Snapshot?
# MAGIC
# MAGIC **The source tables (Dim_Street, Fact_Street_Conditions, gold_street_risk_summary) are DLT materialized views.** Per Databricks documentation:
# MAGIC
# MAGIC > ABAC row filter/column mask policies on streaming tables and materialized views are evaluated **once at pipeline refresh time** using the pipeline's run-as identity, NOT per-viewer at query time.
# MAGIC
# MAGIC This means if we applied policies directly to the DLT objects:
# MAGIC - The policy would be evaluated once using the pipeline owner's identity
# MAGIC - The filtered/masked result would be **permanently baked into the materialized view**
# MAGIC - Every subsequent viewer (admin and restricted alike) would see the same pre-filtered data
# MAGIC - This defeats the entire purpose of role-based access control
# MAGIC
# MAGIC **Solution:** Create a **governed snapshot table** (plain Delta, not DLT) that copies current street data. Apply ABAC policies to this snapshot, which gets true dynamic per-viewer evaluation.
# MAGIC
# MAGIC **Limitation:** This snapshot is frozen at build time and won't reflect later pipeline runs until rebuilt. A production implementation would add a rebuild step to the Day 8-9 orchestration work.
# MAGIC
# MAGIC ## Access Control by Group
# MAGIC
# MAGIC **developer** (Full Access):
# MAGIC - All rows visible (including dangerous >= 0.5)
# MAGIC - Real numeric dangerous rating values visible
# MAGIC - Can create/modify objects
# MAGIC
# MAGIC **safety_team** (Full Read Access):
# MAGIC - All rows visible (including dangerous >= 0.5)
# MAGIC - Real numeric dangerous rating values visible
# MAGIC - Read-only access
# MAGIC
# MAGIC **general_analysts** (Restricted Access):
# MAGIC - **Row Filter**: Streets with dangerous >= 0.5 are hidden entirely
# MAGIC - **Column Mask**: Dangerous rating shown as bucketed labels:
# MAGIC   - **Low**: 0.1 - 0.3
# MAGIC   - **Medium**: 0.3 - 0.5
# MAGIC - Read-only access
# MAGIC
# MAGIC ## Security Implementation Approach
# MAGIC
# MAGIC **First Choice:** ABAC with governed tags
# MAGIC - Reuse existing `business_term: Dangerous Rating` governed tag
# MAGIC - Create row filter and column mask policies via Catalog Explorer UI
# MAGIC - Requires serverless compute (available) or DBR 16.4+
# MAGIC
# MAGIC **Fallback:** Classic functions
# MAGIC - If ABAC policy creation UI not available on this workspace
# MAGIC - Use CREATE FUNCTION + ALTER TABLE ... SET ROW FILTER/MASK
# MAGIC
# MAGIC ## Current User & Testing Approach
# MAGIC **Current user:** rohitrathodcomp@gmail.com (member of **all three** groups: developer, safety_team, general_analysts)
# MAGIC
# MAGIC **Testing strategy:** Since no second identity is available, we'll use temporary group removal:
# MAGIC 1. Capture unrestricted view (current state: member of developer + safety_team)
# MAGIC 2. Temporarily remove from developer and safety_team (keep only general_analysts)
# MAGIC 3. Capture restricted view with real query results
# MAGIC 4. Restore original memberships

# COMMAND ----------

# DBTITLE 1,Create Row Filter Function
# MAGIC %sql
# MAGIC -- Step 1: Verify current user and group memberships
# MAGIC
# MAGIC SELECT current_user() as current_user;
# MAGIC
# MAGIC SELECT 
# MAGIC   is_account_group_member('developer') as is_developer,
# MAGIC   is_account_group_member('safety_team') as is_safety_team,
# MAGIC   is_account_group_member('general_analysts') as is_general_analysts;
# MAGIC
# MAGIC -- Explore source tables
# MAGIC SELECT 
# MAGIC   'dim_street' as table_name,
# MAGIC   COUNT(*) as total_rows,
# MAGIC   COUNT(CASE WHEN is_current = TRUE THEN 1 END) as current_rows,
# MAGIC   COUNT(CASE WHEN is_current = TRUE AND dangerous >= 0.5 THEN 1 END) as current_dangerous_streets,
# MAGIC   COUNT(CASE WHEN is_current = TRUE AND dangerous < 0.5 THEN 1 END) as current_safe_streets
# MAGIC FROM vstone_traffic_dev.dev_rohitrathodcomp_gold.dim_street;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Column Mask Function
# MAGIC %sql
# MAGIC -- Step 2: Create governed snapshot table
# MAGIC -- This is a plain Delta table (NOT a DLT materialized view) so ABAC policies
# MAGIC -- will be evaluated dynamically per-viewer at query time
# MAGIC
# MAGIC DROP TABLE IF EXISTS vstone_traffic_dev.dev_rohitrathodcomp_gold.street_safety_governed;
# MAGIC
# MAGIC CREATE TABLE vstone_traffic_dev.dev_rohitrathodcomp_gold.street_safety_governed
# MAGIC COMMENT 'Governed snapshot of current street safety data for ABAC policy enforcement. Snapshot is frozen at build time and requires rebuild to reflect new pipeline data.'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   street_key,
# MAGIC   street,
# MAGIC   dangerous,
# MAGIC   is_current,
# MAGIC   __START_AT,
# MAGIC   __END_AT
# MAGIC FROM vstone_traffic_dev.dev_rohitrathodcomp_gold.dim_street
# MAGIC WHERE is_current = TRUE;
# MAGIC
# MAGIC -- Verify snapshot creation
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_streets,
# MAGIC   COUNT(CASE WHEN dangerous >= 0.5 THEN 1 END) as dangerous_streets,
# MAGIC   COUNT(CASE WHEN dangerous < 0.5 THEN 1 END) as safe_streets,
# MAGIC   MIN(dangerous) as min_rating,
# MAGIC   MAX(dangerous) as max_rating
# MAGIC FROM vstone_traffic_dev.dev_rohitrathodcomp_gold.street_safety_governed;

# COMMAND ----------

# DBTITLE 1,Create Secured Table Copies
# MAGIC %sql
# MAGIC # Step 3: Check for existing governed tags
# MAGIC # We need to verify if the 'business_term: Dangerous Rating' tag exists
# MAGIC # and can be applied to the dangerous column
# MAGIC
# MAGIC try:
# MAGIC     # Check table tags
# MAGIC     table_tags = spark.sql("""
# MAGIC         SELECT * FROM vstone_traffic_dev.information_schema.table_tags 
# MAGIC         WHERE catalog_name = 'vstone_traffic_dev' 
# MAGIC         AND schema_name = 'dev_rohitrathodcomp_gold'
# MAGIC         AND table_name = 'dim_street'
# MAGIC     """).toPandas()
# MAGIC     print("Table tags on dim_street:")
# MAGIC     print(table_tags)
# MAGIC     print()
# MAGIC except Exception as e:
# MAGIC     print(f"Could not query table tags: {e}\n")
# MAGIC
# MAGIC try:
# MAGIC     # Check column tags
# MAGIC     column_tags = spark.sql("""
# MAGIC         SELECT * FROM vstone_traffic_dev.information_schema.column_tags 
# MAGIC         WHERE catalog_name = 'vstone_traffic_dev' 
# MAGIC         AND schema_name = 'dev_rohitrathodcomp_gold'
# MAGIC         AND table_name = 'dim_street'
# MAGIC         AND column_name = 'dangerous'
# MAGIC     """).toPandas()
# MAGIC     print("Column tags on dim_street.dangerous:")
# MAGIC     print(column_tags)
# MAGIC     print()
# MAGIC except Exception as e:
# MAGIC     print(f"Could not query column tags: {e}\n")
# MAGIC
# MAGIC print("Note: If the governed tag 'business_term: Dangerous Rating' exists,")
# MAGIC print("we'll apply it to street_safety_governed.dangerous and then attempt")
# MAGIC print("to create ABAC policies via Catalog Explorer UI.")

# COMMAND ----------

# DBTITLE 1,Apply Row Filter to dim_street
# MAGIC %sql
# MAGIC ## Step 4A: ABAC Approach - Apply Governed Tag
# MAGIC
# MAGIC **This step requires the Catalog Explorer UI** since governed tag assignment and ABAC policy creation through SQL commands is limited.
# MAGIC
# MAGIC ### If the governed tag exists:
# MAGIC
# MAGIC 1. **Apply tag to column:**
# MAGIC    - Navigate to Catalog Explorer → vstone_traffic_dev → dev_rohitrathodcomp_gold → street_safety_governed
# MAGIC    - Click on the `dangerous` column
# MAGIC    - In Tags section, add: `business_term: Dangerous Rating`
# MAGIC
# MAGIC 2. **Create ABAC Row Filter Policy:**
# MAGIC    - Go to table's Permissions/Policies tab
# MAGIC    - Create Row Filter policy
# MAGIC    - Target: columns with tag `business_term: Dangerous Rating`
# MAGIC    - Condition: `dangerous >= 0.5`
# MAGIC    - Applied to: `general_analysts` group
# MAGIC    - Action: Hide rows
# MAGIC
# MAGIC 3. **Create ABAC Column Mask Policy:**
# MAGIC    - Create Column Mask policy
# MAGIC    - Target: columns with tag `business_term: Dangerous Rating`
# MAGIC    - Applied to: `general_analysts` group
# MAGIC    - Mask expression: 
# MAGIC      ```
# MAGIC      CASE 
# MAGIC        WHEN dangerous < 0.3 THEN 'Low'
# MAGIC        WHEN dangerous < 0.5 THEN 'Medium'
# MAGIC        ELSE CAST(dangerous AS STRING)
# MAGIC      END
# MAGIC      ```
# MAGIC
# MAGIC ### If ABAC UI not available:
# MAGIC Proceed to Step 4B for classic function-based implementation.

# COMMAND ----------

# DBTITLE 1,Apply Column Mask to dim_street.dangerous
# MAGIC %sql
# MAGIC -- Step 4B: Classic Approach - Create Row Filter Function
# MAGIC -- Falls back to hand-written functions if ABAC UI not available
# MAGIC
# MAGIC -- Row Filter: Hide streets with dangerous >= 0.5 from general_analysts
# MAGIC -- developer OR safety_team see all rows
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION vstone_traffic_dev.dev_rohitrathodcomp_gold.filter_dangerous_streets(dangerous_rating DOUBLE)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'Row filter for street safety data: hides dangerous streets (>= 0.5) from general_analysts'
# MAGIC RETURN 
# MAGIC   CASE 
# MAGIC     -- Unrestricted access for developer or safety_team
# MAGIC     WHEN is_account_group_member('developer') THEN TRUE
# MAGIC     WHEN is_account_group_member('safety_team') THEN TRUE
# MAGIC     -- Restricted access for general_analysts: only see safe streets
# MAGIC     WHEN dangerous_rating < 0.5 THEN TRUE
# MAGIC     -- Hide dangerous streets from general_analysts
# MAGIC     ELSE FALSE
# MAGIC   END;
# MAGIC
# MAGIC -- Verify function creation
# MAGIC SELECT 'Row filter function created successfully' as status;

# COMMAND ----------

# DBTITLE 1,Apply Policies to fact_street_conditions
# MAGIC %sql
# MAGIC -- Step 4C: Classic Approach - Create Column Mask Function
# MAGIC -- Replace numeric dangerous rating with bucketed labels for general_analysts
# MAGIC -- developer OR safety_team see real numbers
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION vstone_traffic_dev.dev_rohitrathodcomp_gold.mask_dangerous_rating(dangerous_rating DOUBLE)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Column mask for dangerous rating: shows labels to general_analysts, numbers to developer/safety_team'
# MAGIC RETURN 
# MAGIC   CASE 
# MAGIC     -- Unrestricted access: show real numeric values
# MAGIC     WHEN is_account_group_member('developer') THEN CAST(dangerous_rating AS STRING)
# MAGIC     WHEN is_account_group_member('safety_team') THEN CAST(dangerous_rating AS STRING)
# MAGIC     -- Restricted access: show bucketed labels
# MAGIC     -- Note: Only Low/Medium branches execute for general_analysts since row filter
# MAGIC     -- already hides all dangerous >= 0.5 rows before the mask is evaluated
# MAGIC     WHEN dangerous_rating IS NULL THEN NULL
# MAGIC     WHEN dangerous_rating < 0.3 THEN 'Low'
# MAGIC     WHEN dangerous_rating < 0.5 THEN 'Medium'
# MAGIC     -- These branches are unreachable for general_analysts due to row filter
# MAGIC     ELSE CAST(dangerous_rating AS STRING)
# MAGIC   END;
# MAGIC
# MAGIC -- Verify function creation
# MAGIC SELECT 'Column mask function created successfully' as status;

# COMMAND ----------

# DBTITLE 1,Apply Policies to gold_street_risk_summary
# MAGIC %sql
# MAGIC -- Step 5: Apply row filter and column mask to governed snapshot table
# MAGIC -- This approach works whether ABAC or classic functions were used
# MAGIC
# MAGIC -- Apply row filter to hide dangerous streets from general_analysts
# MAGIC ALTER TABLE vstone_traffic_dev.dev_rohitrathodcomp_gold.street_safety_governed 
# MAGIC SET ROW FILTER vstone_traffic_dev.dev_rohitrathodcomp_gold.filter_dangerous_streets ON (dangerous);
# MAGIC
# MAGIC -- Apply column mask to replace numbers with labels for general_analysts
# MAGIC ALTER TABLE vstone_traffic_dev.dev_rohitrathodcomp_gold.street_safety_governed 
# MAGIC ALTER COLUMN dangerous 
# MAGIC SET MASK vstone_traffic_dev.dev_rohitrathodcomp_gold.mask_dangerous_rating;
# MAGIC
# MAGIC SELECT 'Row filter and column mask applied to street_safety_governed' as status;

# COMMAND ----------

# DBTITLE 1,Grant Permissions
# MAGIC %sql
# MAGIC -- Step 6: Grant permissions to the three groups
# MAGIC -- Note: This catalog (metastore version 1.0) doesn't support USAGE privilege
# MAGIC -- Grants are applied directly at schema and table level
# MAGIC
# MAGIC -- Grant schema-level access
# MAGIC GRANT USE SCHEMA ON SCHEMA vstone_traffic_dev.dev_rohitrathodcomp_gold TO `developer`;
# MAGIC GRANT USE SCHEMA ON SCHEMA vstone_traffic_dev.dev_rohitrathodcomp_gold TO `safety_team`;
# MAGIC GRANT USE SCHEMA ON SCHEMA vstone_traffic_dev.dev_rohitrathodcomp_gold TO `general_analysts`;
# MAGIC
# MAGIC -- Grant table permissions
# MAGIC -- developer: full access including CREATE/MODIFY
# MAGIC GRANT ALL PRIVILEGES ON TABLE vstone_traffic_dev.dev_rohitrathodcomp_gold.street_safety_governed TO `developer`;
# MAGIC
# MAGIC -- safety_team: SELECT only (unrestricted view - policies check for developer OR safety_team)
# MAGIC GRANT SELECT ON TABLE vstone_traffic_dev.dev_rohitrathodcomp_gold.street_safety_governed TO `safety_team`;
# MAGIC
# MAGIC -- general_analysts: SELECT only (restricted by row filter and column mask)
# MAGIC GRANT SELECT ON TABLE vstone_traffic_dev.dev_rohitrathodcomp_gold.street_safety_governed TO `general_analysts`;
# MAGIC
# MAGIC SELECT 'Permissions granted successfully' as status;

# COMMAND ----------

# DBTITLE 1,Verification Setup
# MAGIC %md
# MAGIC # Verification Queries
# MAGIC
# MAGIC ## Testing Approach
# MAGIC
# MAGIC Since **rohitrathodcomp@gmail.com is a member of all three groups** (developer, safety_team, general_analysts), we'll use a **temporary group removal approach** to demonstrate the restricted view:
# MAGIC
# MAGIC ### Step 1: Current State (Unrestricted View)
# MAGIC Run the queries below **now** while you're a member of developer + safety_team + general_analysts.
# MAGIC
# MAGIC You should see:
# MAGIC - All streets visible (including dangerous >= 0.5)
# MAGIC - Real numeric dangerous rating values
# MAGIC
# MAGIC ### Step 2: Restricted View (general_analysts only)
# MAGIC To capture the restricted view:
# MAGIC
# MAGIC 1. **Workspace Settings → Identity and access → Groups**
# MAGIC 2. Remove rohitrathodcomp@gmail.com from `developer` group
# MAGIC 3. Remove rohitrathodcomp@gmail.com from `safety_team` group
# MAGIC 4. Keep membership in `general_analysts` group only
# MAGIC 5. **Start a new SQL execution context** (refresh the page or restart serverless compute)
# MAGIC 6. Re-run the same verification queries below
# MAGIC 7. Capture the output showing:
# MAGIC    - Reduced row counts (dangerous streets hidden)
# MAGIC    - "Low" / "Medium" labels instead of numeric values
# MAGIC
# MAGIC ### Step 3: Restore Memberships
# MAGIC After capturing the restricted view, restore memberships to developer and safety_team.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Verification Queries (Run in Both States)

# COMMAND ----------

# DBTITLE 1,Verify dim_street - Admin View
# MAGIC %sql
# MAGIC -- Verification 1: Check current group membership
# MAGIC -- Run this in BOTH states (before and after group removal)
# MAGIC
# MAGIC SELECT 
# MAGIC   current_user() as user,
# MAGIC   is_account_group_member('developer') as is_developer,
# MAGIC   is_account_group_member('safety_team') as is_safety_team,
# MAGIC   is_account_group_member('general_analysts') as is_general_analysts;
# MAGIC
# MAGIC -- Count visible streets in governed snapshot
# MAGIC -- EXPECTED RESULTS:
# MAGIC -- - Unrestricted (developer OR safety_team): 36 total streets visible
# MAGIC -- - Restricted (general_analysts only): ~14 safe streets visible (dangerous < 0.5)
# MAGIC
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_streets_visible,
# MAGIC   MIN(dangerous) as min_dangerous_value,
# MAGIC   MAX(dangerous) as max_dangerous_value
# MAGIC FROM vstone_traffic_dev.dev_rohitrathodcomp_gold.street_safety_governed;

# COMMAND ----------

# DBTITLE 1,Verify gold_street_risk_summary - Admin View
# MAGIC %sql
# MAGIC -- Verification 2: Sample rows showing dangerous column values
# MAGIC -- Run this in BOTH states (before and after group removal)
# MAGIC
# MAGIC -- EXPECTED RESULTS:
# MAGIC -- - Unrestricted: dangerous column shows numeric values like '0.1', '0.3', '0.6', '0.8'
# MAGIC -- - Restricted: dangerous column shows labels 'Low' or 'Medium' only
# MAGIC
# MAGIC SELECT 
# MAGIC   street_key,
# MAGIC   street,
# MAGIC   dangerous as dangerous_value_or_label
# MAGIC FROM vstone_traffic_dev.dev_rohitrathodcomp_gold.street_safety_governed
# MAGIC ORDER BY street_key
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Verify fact_street_conditions - Admin View
# MAGIC %sql
# MAGIC -- Verification 3: Distribution of dangerous values
# MAGIC -- Run this in BOTH states (before and after group removal)
# MAGIC
# MAGIC -- EXPECTED RESULTS:
# MAGIC -- Unrestricted: Shows actual numeric ranges and counts
# MAGIC -- Restricted: All values appear as 'Low' or 'Medium' strings, no numeric values
# MAGIC
# MAGIC SELECT 
# MAGIC   dangerous as value_or_label,
# MAGIC   COUNT(*) as street_count
# MAGIC FROM vstone_traffic_dev.dev_rohitrathodcomp_gold.street_safety_governed
# MAGIC GROUP BY dangerous
# MAGIC ORDER BY dangerous;

# COMMAND ----------

# DBTITLE 1,✅ RESTRICTED VIEW - Results Captured
# MAGIC %md
# MAGIC ## ✅ RESTRICTED VIEW - Actual Results Captured
# MAGIC
# MAGIC ### Current State (After Group Removal)
# MAGIC
# MAGIC **User:** rohitrathodcomp@gmail.com  
# MAGIC **Group Memberships:** ~~developer~~ | ~~safety_team~~ | general_analysts ✓ **ONLY**
# MAGIC
# MAGIC ### Verification Results
# MAGIC
# MAGIC #### 1. Row Count
# MAGIC ```
# MAGIC total_streets_visible: 14  ← DOWN FROM 36 (61% reduction)
# MAGIC min_dangerous_value: "Low"  ← STRING label, not numeric
# MAGIC max_dangerous_value: "Medium"  ← STRING label, not numeric
# MAGIC ```
# MAGIC
# MAGIC **✅ Row Filter Working:** All 22 dangerous streets (>= 0.5) successfully hidden
# MAGIC
# MAGIC #### 2. Sample Dangerous Values (All 14 Visible Rows)
# MAGIC Only STRING labels visible:
# MAGIC - **"Low"** - appears 12 times
# MAGIC - **"Medium"** - appears 2 times
# MAGIC
# MAGIC No numeric values appear at all. Streets shown:
# MAGIC - Corts Valencianes 2A/2B (Low)
# MAGIC - PalasietB (Medium)
# MAGIC - Aben FerriA/B (Low)
# MAGIC - Argenteria (Medium)
# MAGIC - 9 OctubreA/B (Low)
# MAGIC - Beata InesA/B (Low)
# MAGIC - Albereda 1/2 (Low)
# MAGIC - N-340A/B (Low)
# MAGIC
# MAGIC **✅ Column Mask Working:** All numeric dangerous ratings replaced with bucketed labels
# MAGIC
# MAGIC #### 3. Distribution by Dangerous Rating
# MAGIC ```
# MAGIC "Low" → 12 streets  (dangerous 0.1-0.3)
# MAGIC "Medium" → 2 streets  (dangerous 0.3-0.5)
# MAGIC
# MAGIC Total visible: 14 streets
# MAGIC Hidden: 22 streets (all dangerous >= 0.5)
# MAGIC ```
# MAGIC
# MAGIC **✅ Only 2 distinct values** instead of the unrestricted view's 7
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Before/After Comparison Summary
# MAGIC
# MAGIC | Metric | Unrestricted (developer/safety_team) | Restricted (general_analysts only) | Change |
# MAGIC |--------|-------------------------------------|-----------------------------------|--------|
# MAGIC | **Visible Streets** | 36 | 14 | **-61%** |
# MAGIC | **Dangerous Column Type** | STRING (numeric) | STRING (labels) | Changed |
# MAGIC | **Dangerous Values** | 0.1, 0.2, 0.3, 0.5, 0.7, 0.8, 0.9 | "Low", "Medium" | **Masked** |
# MAGIC | **Distinct Values** | 7 | 2 | **-71%** |
# MAGIC | **Streets >= 0.5 visible** | 22 (all) | 0 (hidden) | **Filtered** |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Security Implementation Validated
# MAGIC
# MAGIC ### Row Filter Function
# MAGIC ```sql
# MAGIC filter_dangerous_streets(dangerous_rating DOUBLE) RETURNS BOOLEAN
# MAGIC ```
# MAGIC **Verified behavior:**
# MAGIC - developer OR safety_team → Returns TRUE for ALL rows
# MAGIC - general_analysts → Returns TRUE only if dangerous < 0.5
# MAGIC - **Result:** 22 dangerous streets completely hidden from general_analysts
# MAGIC
# MAGIC ### Column Mask Function
# MAGIC ```sql
# MAGIC mask_dangerous_rating(dangerous_rating DOUBLE) RETURNS STRING
# MAGIC ```
# MAGIC **Verified behavior:**
# MAGIC - developer OR safety_team → Returns CAST(dangerous AS STRING) (numeric)
# MAGIC - general_analysts → Returns 'Low' (< 0.3) or 'Medium' (0.3-0.5)
# MAGIC - **Result:** No numeric values visible to general_analysts
# MAGIC
# MAGIC ### Access Control Works As Designed
# MAGIC ✅ **developer** - Full unrestricted access  
# MAGIC ✅ **safety_team** - Full unrestricted read access  
# MAGIC ✅ **general_analysts** - Restricted (filtered + masked)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Restore Group Memberships
# MAGIC
# MAGIC **Testing complete!** Now restore the original memberships:
# MAGIC
# MAGIC 1. **Workspace Settings** → **Identity and access** → **Groups**
# MAGIC 2. Click on **developer** group → Add rohitrathodcomp@gmail.com
# MAGIC 3. Click on **safety_team** group → Add rohitrathodcomp@gmail.com
# MAGIC 4. Verify membership in all three groups restored

# COMMAND ----------

# DBTITLE 1,UNRESTRICTED VIEW - Baseline Results Captured
# MAGIC %md
# MAGIC ## ✅ UNRESTRICTED VIEW - Baseline Results Captured
# MAGIC
# MAGIC ### Current State (Before Group Removal)
# MAGIC
# MAGIC **User:** rohitrathodcomp@gmail.com  
# MAGIC **Group Memberships:** developer ✓ | safety_team ✓ | general_analysts ✓
# MAGIC
# MAGIC ### Verification Results
# MAGIC
# MAGIC #### 1. Row Count
# MAGIC ```
# MAGIC total_streets_visible: 36
# MAGIC min_dangerous_value: "0.1"
# MAGIC max_dangerous_value: "0.9"
# MAGIC ```
# MAGIC
# MAGIC #### 2. Sample Dangerous Values (First 20 Rows)
# MAGIC Numeric STRING values visible:
# MAGIC - "0.5", "0.7", "0.1", "0.3", "0.8", "0.2", "0.9"
# MAGIC - Real DOUBLE values converted to strings by the column mask function
# MAGIC - Full range from 0.1 to 0.9 visible
# MAGIC
# MAGIC #### 3. Distribution by Dangerous Rating
# MAGIC ```
# MAGIC 0.1 → 8 streets
# MAGIC 0.2 → 4 streets
# MAGIC 0.3 → 2 streets
# MAGIC 0.5 → 10 streets  ← dangerous cutoff starts here
# MAGIC 0.7 → 5 streets
# MAGIC 0.8 → 6 streets
# MAGIC 0.9 → 1 street
# MAGIC
# MAGIC Total: 36 streets
# MAGIC Safe (< 0.5): 14 streets
# MAGIC Dangerous (>= 0.5): 22 streets
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Next: Capture RESTRICTED VIEW
# MAGIC
# MAGIC **To demonstrate the restricted view for general_analysts:**
# MAGIC
# MAGIC ### Step 1: Remove from Developer and Safety Team Groups
# MAGIC 1. Open **Workspace Settings** → **Identity and access** → **Groups**
# MAGIC 2. Click on **developer** group → Remove rohitrathodcomp@gmail.com
# MAGIC 3. Click on **safety_team** group → Remove rohitrathodcomp@gmail.com
# MAGIC 4. Leave in **general_analysts** group only
# MAGIC
# MAGIC ### Step 2: Refresh Execution Context
# MAGIC 5. **Important:** Refresh this notebook page or restart serverless compute
# MAGIC 6. Group membership is cached per session - need fresh context
# MAGIC
# MAGIC ### Step 3: Re-run Verification Queries
# MAGIC 7. Re-run Verification cells 1, 2, and 3 above
# MAGIC 8. **Expected changes:**
# MAGIC    - Row count drops from 36 to ~14 (dangerous streets hidden)
# MAGIC    - Dangerous values show as **"Low"** or **"Medium"** labels only
# MAGIC    - Distribution shows only 2 distinct values instead of 7
# MAGIC
# MAGIC ### Step 4: Restore Memberships
# MAGIC 9. Add rohitrathodcomp@gmail.com back to **developer** group
# MAGIC 10. Add rohitrathodcomp@gmail.com back to **safety_team** group

# COMMAND ----------

# DBTITLE 1,Implementation Summary
# MAGIC %md
# MAGIC # Implementation Summary
# MAGIC
# MAGIC ## ✅ Successfully Implemented
# MAGIC
# MAGIC Unity Catalog row filter and column mask have been applied to the governed snapshot table:
# MAGIC
# MAGIC ### street_safety_governed
# MAGIC
# MAGIC **Table Type:** Plain Delta table (NOT a DLT materialized view)
# MAGIC - Contains snapshot of current street safety data from Dim_Street
# MAGIC - Policies are evaluated **dynamically per-viewer** at query time
# MAGIC - Safe to use for role-based access control
# MAGIC
# MAGIC **Row Filter Applied:**
# MAGIC - Function: `filter_dangerous_streets(dangerous)`
# MAGIC - Logic: Hide rows where `dangerous >= 0.5` from `general_analysts`
# MAGIC - Unrestricted: `developer` OR `safety_team` see all rows
# MAGIC
# MAGIC **Column Mask Applied:**
# MAGIC - Function: `mask_dangerous_rating(dangerous)`
# MAGIC - Logic: Replace numeric values with "Low"/"Medium" labels for `general_analysts`
# MAGIC - Unrestricted: `developer` OR `safety_team` see raw numbers
# MAGIC
# MAGIC ## Access Control Groups
# MAGIC
# MAGIC Three custom groups are active on this workspace:
# MAGIC
# MAGIC 1. **developer** - Full access (unrestricted view + CREATE/MODIFY)
# MAGIC 2. **safety_team** - Full read access (unrestricted view, read-only)
# MAGIC 3. **general_analysts** - Restricted read access (row filter + column mask)
# MAGIC
# MAGIC ## Current Implementation Mechanism
# MAGIC
# MAGIC **Classic Functions Approach** (used)
# MAGIC - Hand-written SQL functions for row filter and column mask
# MAGIC - Applied via `ALTER TABLE ... SET ROW FILTER` and `ALTER COLUMN ... SET MASK`
# MAGIC - Functions use `is_account_group_member()` to check group membership
# MAGIC - **Functions created:**
# MAGIC   - `filter_dangerous_streets(dangerous_rating)` - row filter
# MAGIC   - `mask_dangerous_rating(dangerous_rating)` - column mask
# MAGIC
# MAGIC **Why Not ABAC?**
# MAGIC - ABAC governed tag approach requires UI-based policy creation in Catalog Explorer
# MAGIC - Catalog Explorer policy creation steps documented in notebook (Step 4A)
# MAGIC - Classic function approach provides identical functionality and is fully SQL-based
# MAGIC - Can be migrated to ABAC later if needed by retagging the column and creating policies via UI
# MAGIC
# MAGIC ## Workspace Limitation Encountered
# MAGIC
# MAGIC **Metastore Version 1.0 Limitation:**
# MAGIC - `GRANT USAGE ON CATALOG` failed with error: "Privilege USAGE is not applicable to this entity"
# MAGIC - **Workaround:** Used `GRANT USE SCHEMA` instead of catalog-level grants
# MAGIC - This is a known limitation of older Unity Catalog metastore versions
# MAGIC - Functionally equivalent - schema-level grants provide the needed access
# MAGIC
# MAGIC ## Production Readiness Notes
# MAGIC
# MAGIC ⚠️ **Snapshot Freshness**: The `street_safety_governed` table is a point-in-time snapshot. It does NOT automatically refresh when:
# MAGIC - New data arrives in Silver (e.g., Day 7's MERGE INTO silver_environment)
# MAGIC - Dim_Street gets new SCD2 versions from the DLT pipeline
# MAGIC
# MAGIC **Recommendation**: Add a scheduled rebuild step as part of Day 8-9 orchestration:
# MAGIC ```sql
# MAGIC TRUNCATE TABLE street_safety_governed;
# MAGIC INSERT INTO street_safety_governed
# MAGIC SELECT street_key, street, dangerous, is_current, valid_from, valid_to
# MAGIC FROM dim_street WHERE is_current = TRUE;
# MAGIC ```
# MAGIC
# MAGIC ## Testing Status
# MAGIC
# MAGIC **Current user:** rohitrathodcomp@gmail.com
# MAGIC **Current memberships:** developer + safety_team + general_analysts (all three)
# MAGIC **Current view:** Unrestricted (due to developer/safety_team membership)
# MAGIC
# MAGIC **To capture restricted view:**
# MAGIC 1. Remove user from developer and safety_team groups
# MAGIC 2. Keep only general_analysts membership
# MAGIC 3. Refresh execution context
# MAGIC 4. Re-run verification queries
# MAGIC 5. Restore memberships after testing

# COMMAND ----------

# DBTITLE 1,Expected Results - Users Group View
# MAGIC %md
# MAGIC ## Expected Results - Before/After Comparison
# MAGIC
# MAGIC ### BEFORE: Unrestricted View (developer OR safety_team membership)
# MAGIC
# MAGIC **Group Membership Check:**
# MAGIC ```
# MAGIC is_developer: true
# MAGIC is_safety_team: true  
# MAGIC is_general_analysts: true
# MAGIC ```
# MAGIC
# MAGIC **Row Count:**
# MAGIC ```
# MAGIC total_streets_visible: 36
# MAGIC ```
# MAGIC
# MAGIC **Sample Dangerous Values:**
# MAGIC ```
# MAGIC 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9
# MAGIC (actual numeric DOUBLE values)
# MAGIC ```
# MAGIC
# MAGIC **Distribution:**
# MAGIC Multiple distinct numeric values across the 0.1-0.9 range
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### AFTER: Restricted View (general_analysts membership ONLY)
# MAGIC
# MAGIC **To capture this view:**
# MAGIC 1. Workspace Settings → Identity and access → Groups
# MAGIC 2. Remove rohitrathodcomp@gmail.com from `developer`
# MAGIC 3. Remove rohitrathodcomp@gmail.com from `safety_team`
# MAGIC 4. Keep in `general_analysts` only
# MAGIC 5. Refresh page or restart compute
# MAGIC 6. Re-run verification queries
# MAGIC
# MAGIC **Group Membership Check:**
# MAGIC ```
# MAGIC is_developer: false
# MAGIC is_safety_team: false
# MAGIC is_general_analysts: true
# MAGIC ```
# MAGIC
# MAGIC **Row Count:**
# MAGIC ```
# MAGIC total_streets_visible: ~14 (exact count depends on data)
# MAGIC ```
# MAGIC
# MAGIC **Sample Dangerous Values:**
# MAGIC ```
# MAGIC 'Low', 'Medium'
# MAGIC (STRING labels only, NO numeric values)
# MAGIC ```
# MAGIC
# MAGIC **Distribution:**
# MAGIC Only two distinct values: 'Low' and 'Medium'
# MAGIC All streets with dangerous >= 0.5 are hidden by the row filter
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Validation Checklist
# MAGIC
# MAGIC ### ✅ Row Filter Working:
# MAGIC - [ ] Unrestricted: 36 streets visible
# MAGIC - [ ] Restricted: ~14 streets visible (only dangerous < 0.5)
# MAGIC - [ ] Restricted: No streets with dangerous >= 0.5 appear at all
# MAGIC
# MAGIC ### ✅ Column Mask Working:
# MAGIC - [ ] Unrestricted: Numeric values like 0.1, 0.3, 0.6, 0.8 visible
# MAGIC - [ ] Restricted: Only string labels 'Low' and 'Medium' visible
# MAGIC - [ ] Restricted: No numeric values appear
# MAGIC
# MAGIC ### ✅ Group Logic Working:
# MAGIC - [ ] developer membership grants unrestricted access
# MAGIC - [ ] safety_team membership grants unrestricted access
# MAGIC - [ ] general_analysts ALONE gets restricted access
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## After Testing: Restore Memberships
# MAGIC
# MAGIC **IMPORTANT:** After capturing the restricted view output:
# MAGIC
# MAGIC 1. Workspace Settings → Identity and access → Groups
# MAGIC 2. Add rohitrathodcomp@gmail.com back to `developer`
# MAGIC 3. Add rohitrathodcomp@gmail.com back to `safety_team`
# MAGIC 4. Verify all three group memberships are restored

# COMMAND ----------

# DBTITLE 1,Why Not Apply Policies Directly to DLT Tables?
# MAGIC %md
# MAGIC ## ⛔ Why Not Apply Policies Directly to DLT Materialized Views?
# MAGIC
# MAGIC ### The Problem with Policies on Streaming Tables / Materialized Views
# MAGIC
# MAGIC Per [Databricks documentation on ABAC policies](https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html#considerations):
# MAGIC
# MAGIC > Row filters and column masks on streaming tables and materialized views are **evaluated once at pipeline refresh time** using the pipeline's run-as identity.
# MAGIC
# MAGIC This means:
# MAGIC
# MAGIC 1. **Policy is evaluated ONCE** during pipeline refresh
# MAGIC 2. **Uses the pipeline owner's identity** (not the viewer's)
# MAGIC 3. **Result is permanently baked** into the materialized view
# MAGIC 4. **Every subsequent viewer** sees the same pre-filtered/masked data
# MAGIC
# MAGIC ### Why This Defeats Role-Based Access Control
# MAGIC
# MAGIC If we had applied policies directly to the DLT objects:
# MAGIC
# MAGIC ```sql
# MAGIC -- THIS WOULD BE WRONG:
# MAGIC ALTER TABLE dim_street  -- DLT materialized view
# MAGIC SET ROW FILTER filter_dangerous_streets ON (dangerous);
# MAGIC ```
# MAGIC
# MAGIC **What would happen:**
# MAGIC - Pipeline runs as the pipeline owner (likely rohitrathodcomp@gmail.com)
# MAGIC - Row filter checks: "Is pipeline owner in developer/safety_team?" → YES
# MAGIC - Filter passes ALL rows (including dangerous >= 0.5)
# MAGIC - Materialized view **permanently stores** all 36 streets
# MAGIC - **Problem:** general_analysts users would STILL see all 36 streets
# MAGIC - The row filter would never hide anything for any viewer
# MAGIC
# MAGIC ### The Governed Snapshot Solution
# MAGIC
# MAGIC ```sql
# MAGIC -- THIS IS CORRECT:
# MAGIC CREATE TABLE street_safety_governed  -- Plain Delta table
# MAGIC AS SELECT ... FROM dim_street WHERE is_current = TRUE;
# MAGIC
# MAGIC ALTER TABLE street_safety_governed  -- Policies on plain table
# MAGIC SET ROW FILTER filter_dangerous_streets ON (dangerous);
# MAGIC ```
# MAGIC
# MAGIC **What happens:**
# MAGIC - Query runs against plain Delta table
# MAGIC - Row filter evaluated **per-viewer at query time**
# MAGIC - Checks: "Is THIS viewer in developer/safety_team?"
# MAGIC   - YES → show all rows
# MAGIC   - NO (general_analysts only) → hide dangerous >= 0.5
# MAGIC - **Result:** Different users see different data from the same table
# MAGIC
# MAGIC ### Trade-Off: Snapshot Freshness
# MAGIC
# MAGIC **Limitation:** The governed snapshot is frozen at creation time.
# MAGIC
# MAGIC **Impact:**
# MAGIC - New streets added to Dim_Street won't appear in street_safety_governed
# MAGIC - SCD2 updates changing dangerous ratings won't reflect
# MAGIC - Day 7's MERGE INTO silver_environment changes won't propagate
# MAGIC
# MAGIC **Mitigation:** Add a scheduled rebuild to the Day 8-9 orchestration:
# MAGIC ```sql
# MAGIC TRUNCATE TABLE street_safety_governed;
# MAGIC INSERT INTO street_safety_governed
# MAGIC SELECT street_key, street, dangerous, is_current, __START_AT, __END_AT
# MAGIC FROM dim_street WHERE is_current = TRUE;
# MAGIC ```
# MAGIC
# MAGIC ### Summary
# MAGIC
# MAGIC ✅ **Governed snapshot (plain Delta table):**
# MAGIC - Dynamic per-viewer evaluation ✓
# MAGIC - True role-based access control ✓
# MAGIC - Requires periodic refresh ⚠️
# MAGIC
# MAGIC ❌ **Direct policy on DLT materialized view:**
# MAGIC - One-time evaluation at refresh ❌
# MAGIC - Fixed result for all viewers ❌
# MAGIC - Defeats purpose of RBAC ❌

