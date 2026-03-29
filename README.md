# Olist E-Commerce Analytics & Churn Prediction

---

## Case Study

How can an e-commerce platform:

- Track business growth and customer behaviour?
- Identify operational inefficiencies in delivery?
- Detect high-risk sellers impacting customer experience?
- Predict which customers are likely to churn — and why?

---

## About the Dataset (Olist E-Commerce)

This project uses the **Brazilian Olist E-Commerce dataset**, containing real-world transactional data across the full e-commerce lifecycle.

- ~100,000 orders  
- ~99,000 customers  
- ~3,000 sellers  
- ~100,000 reviews  

### Data Coverage:
- Orders, payments, reviews  
- Delivery timelines  
- Seller and product information  

This enables **end-to-end analytics from operations → customer experience → churn prediction**.

---

## Project Architecture

![Databricks UI](screenshots/Databricks.jpg)

Databricks (Bronze → Silver → Gold)
↓
dbt (Data Modeling + Testing)
↓
Machine Learning (XGBoost Churn Model)
↓
Power BI Dashboard (Business Insights)

---

## Tech Stack

- Databricks (PySpark, Delta Lake)
- dbt (Transformation + Data Quality Testing)
- Python (Scikit-learn, XGBoost)
- MLflow (Model tracking & registry)
- Power BI (Business dashboards)

---

# DASHBOARDS (BUSINESS-FIRST APPROACH)

---

## 1️⃣ Business Overview

![Business Overview](screenshots/01_business_overview.png)

### Key Observations:
- Revenue scaled to **$15.4M+** with steady growth trend  
- Order volume reached **96K+ transactions**  
- Customer base expanded to **~96K unique customers**  
- Early signs of increasing delivery variability observed  

---

## 2️⃣ Delivery Performance

![Delivery Performance](screenshots/02_delivery_performance.png)

### Key Observations:
- **10.4% average late delivery rate** across Brazil  
- Significant regional disparities in delivery performance  
- Clear negative correlation between **late delivery % and review scores**  
- Despite average early deliveries (-13 days), variability drives dissatisfaction  

---

## 3️⃣ Seller Risk Analysis

![Seller Risk](screenshots/03_seller_risk.png)

### Key Observations:
- Only **~1% sellers classified as high-risk**, yet drive disproportionate delays  
- Top sellers show **50–65% late delivery rates**  
- Strong relationship between **seller delay and customer satisfaction drop**  
- Identifies precise targets for operational intervention  

---

## 4️⃣ Customer Churn Prediction (ML Output)

![Churn Dashboard](screenshots/04_customer_churn.png)

### Key Observations:
- **~81% customers classified as high churn risk**  
- Strong alignment between predicted churn probability and actual outcomes  
- Enables proactive targeting of high-risk customers  
- Clear link between delivery issues and churn likelihood  

---

# MACHINE LEARNING — CHURN MODEL

---

## Models Used
- Logistic Regression (Baseline)
- XGBoost (Final Model)

---

## ROC Curve

![ROC](screenshots/08_roc_curve.png)

- XGBoost achieved **AUC ≈ 0.73**
- Significant improvement over baseline (0.67)

---

## Confusion Matrix

![Confusion](screenshots/05_confusion_matrix_xgboost.png)

- Correctly identified **12,000+ churned customers**
- Acceptable false positives aligned with retention strategy

---

## Churn Distribution

![Distribution](screenshots/06_customer_churn_distribution.png)

- Imbalanced dataset (~70% churn)  
- AUC used as primary evaluation metric  

---

## Feature Importance

![Importance](screenshots/07_feature_importance_xgboost.png)

### Key Drivers of Churn:
- Delivery duration and delays  
- Late order percentage  
- Customer dissatisfaction signals  
- Review scores  

---

## Model Insight

The model is well-calibrated:

- Predicted probabilities closely match actual churn rates  
- Demonstrates strong generalization (not overfitting)  
- Suitable for real-world decision-making  

---

# DATA LINEAGE (DBT DAG)

---

## Full Pipeline DAG

![DBT DAG](screenshots/09_dbt_lineage_graph.png)

---

## Data Modelling Approach

- Layered architecture (Bronze → Silver → Gold)
- Aggregations pushed to mart layer for BI consumption
- Composite keys handled for transactional integrity
- dbt tests implemented:
  - Null checks
  - Uniqueness validation
  - Relationship constraints
- Feature engineering aligned with churn behaviour

---

# Main Insights

This solution enables measurable business value:

### Operations
- Identifies **10%+ late delivery risk zones**
- Pinpoints **specific states and sellers causing delays**
- Enables targeted logistics optimization  

---

### Seller Management
- Detects **top 1% high-risk sellers driving majority of delays**
- Enables **focused performance interventions**
- Reduces operational inefficiency at source  

---

### Customer Retention
- Flags **high-risk churn customers (~81%)**
- Enables **proactive retention campaigns**
- Improves customer lifetime value  

---

### Decision Making
- Connects **operations → customer experience → churn**
- Translates raw data into **actionable business insights**
- Supports **data-driven strategy at scale**

---

# WHAT THIS PROJECT SHOWS

- A small subset of sellers is responsible for a large share of delivery failures  
- Delivery delays — not average delivery time — are the real driver of poor customer experience  
- Customers exposed to inconsistent delivery performance are significantly more likely to churn  

---

# WHAT SHOULD BE DONE (BUSINESS ACTIONS)

- Prioritise intervention on the worst-performing sellers instead of broad optimisations  
- Fix logistics in high-delay states (carrier changes, routing, local capacity)  
- Use churn scores to trigger retention actions immediately after bad delivery experiences  
- Track late delivery rate as a core business KPI linked directly to churn  

---

## Final Outcome

This project demonstrates how an end-to-end data pipeline can:

- Transform raw transactional data into business insights  
- Identify operational bottlenecks  
- Predict customer behaviour  
- Enable measurable business impact  

---

# HOW TO RUN

1. Load raw data into Databricks  
2. Run dbt models:

dbt run 

dbt test

3. Execute ML notebook
4. Connect PowerBI to Gold tables

---

# AUTHOR

Alok T P  

---






