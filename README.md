# 🛒 Olist E-Commerce Analytics & Churn Prediction (End-to-End Data Project)

---

## Case Study — What Are We Trying to Answer?

How can an e-commerce platform:

- Understand business performance over time?
- Identify operational inefficiencies in delivery?
- Detect high-risk sellers impacting customer experience?
- Predict which customers are likely to churn — and why?

---

## Final Business Insight

Operational inefficiencies in delivery performance — driven by a small group of high-risk sellers — lead to poor customer experience and ultimately increase customer churn.

---

## Project Architecture

Databricks (Bronze → Silver → Gold)
↓
dbt (Data Modeling + Testing)
↓
Machine Learning (Churn Prediction - XGBoost)
↓
Power BI Dashboard (Business Insights)

---

## Tech Stack

- **Databricks** (PySpark, Delta Lake)
![Business Overview](screenshots/Databricks.jpg)
  
- **dbt** (Data transformation & testing)
- **Python** (XGBoost, Scikit-learn)
- **Power BI** (Dashboarding)
- **MLflow** (Model tracking & registry)

---

## 1. Business Overview Dashboard

![Business Overview](screenshots/01_business_overview.png)

### Key Insights:
- Revenue and order volume show consistent growth
- Strong business expansion trend
- Emerging fluctuations in delivery performance

---

## 2. Delivery Performance Analysis

![Delivery Performance](screenshots/02_delivery_performance.png)

### Key Insights:
- Certain states exhibit significantly higher late delivery rates
- Delivery delays strongly correlate with lower customer review scores
- Geographic inefficiencies identified

---

## 3. Seller Risk Analysis

![Seller Risk](screenshots/03_seller_risk.png)

### Key Insights:
- A small group of sellers contributes disproportionately to delivery delays
- High-risk sellers directly impact customer satisfaction
- Targeted intervention can improve overall performance

---

## 4. Customer Churn Prediction Dashboard

![Churn Dashboard](screenshots/04_customer_churn.png)

### Key Insights:
- ~81% customers identified as high churn risk
- Strong alignment between predicted churn probability and actual churn
- Enables proactive retention strategies

---

## Machine Learning — Churn Model

### Models Used:
- Logistic Regression (Baseline)
- XGBoost (Final Model)

---

### ROC Curve Comparison

![ROC Curve](screenshots/08_roc_curve.png)

- XGBoost outperforms baseline model
- Captures non-linear relationships effectively

---

### Confusion Matrix (XGBoost)

![Confusion Matrix](screenshots/05_confusion_matrix_xgboost.png)

- Strong ability to identify churned customers
- Some false positives acceptable for retention strategy

---

### Churn Distribution

![Churn Distribution](screenshots/06_customer_churn_distribution.png)

- Imbalanced dataset (~70% churn)
- Justifies use of AUC over accuracy

---

### Feature Importance

![Feature Importance](screenshots/07_feature_importance_xgboost.png)

### Key Drivers of Churn:
- Delivery delays
- Late order percentage
- Delivery duration
- Customer dissatisfaction signals

---

## Model Insight

The model is well-calibrated:
- Predicted probabilities closely match actual churn rates
- Indicates strong generalization, not overfitting

---

## Data Pipeline Highlights

- Built multi-layer architecture (Bronze → Silver → Gold)
- Implemented dbt tests:
  - Null checks
  - Uniqueness constraints
  - Relationship validation
- Handled:
  - Duplicate records
  - Composite key logic
  - Data quality issues

---

## Key Outputs

- Clean analytical data models (Gold layer)
- Customer churn scoring table
- Interactive Power BI dashboard
- Machine learning model with evaluation

---

## Business Impact

This solution enables:

- Identification of operational bottlenecks
- Seller performance monitoring
- Customer churn prediction
- Data-driven retention strategies

---

## How to Run

1. Load raw data into Databricks
2. Run dbt models:

dbt run
dbt test

3. Execute ML notebook
4. Connect Power BI to Gold tables

---

## Author

Alok T P  
---



