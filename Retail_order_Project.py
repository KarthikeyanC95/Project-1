import mysql.connector
import pandas as pd
import streamlit as st

# MySQL connection setup
def connect_to_mysql():
    connection = mysql.connector.connect(
        host="localhost",        # Your MySQL host
        user="root",    # Your MySQL username
        password="mysql",  # Your MySQL password
        database="retail_order"  # Replace with your database name
    )
    return connection

# Function to fetch data from a query
def fetch_data_from_query(query):
    connection = connect_to_mysql()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(result, columns=columns)
    cursor.close()
    connection.close()
    return df

# List of queries and their associated questions
queries = [
    """SELECT p.product_id, o.category, p.Sub_Category, SUM(p.Quantity * p.sale_price) AS revenue FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id GROUP BY p.product_id, o.category, p.Sub_Category ORDER BY revenue DESC LIMIT 10;""",
    """SELECT o.City, SUM((p.Quantity * p.sale_price) - p.Cost_price) / SUM(p.Quantity * p.sale_price) * 100 AS profit_margin FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id GROUP BY o.City ORDER BY profit_margin DESC LIMIT 5;""",
    """SELECT o.Category, sum(p.Discount), sum(Discount_percent) FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id GROUP BY o.Category ORDER BY 1 DESC;""",
    """SELECT o.Category, avg(p.sale_price) FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id GROUP BY o.Category ORDER BY 1 DESC;""",
    """SELECT o.region, avg(p.sale_price) FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id GROUP BY o.region ORDER BY 2 DESC;""",
    """SELECT o.Category, sum(p.Profit) FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id GROUP BY o.Category ORDER BY 1 DESC;""",
    """SELECT o.segment, sum(p.Quantity) FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id GROUP BY o.segment ORDER BY 2 DESC limit 3;""",
    """SELECT o.region, avg(p.Discount_Percent) FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id GROUP BY o.region ORDER BY 2 DESC;""",
    """SELECT o.Category, p.Profit FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id ORDER BY 2 DESC;""",
    """SELECT YEAR(o.Order_Date) AS year, SUM(Sale_price) AS total_revenue FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id WHERE o.Order_Date BETWEEN '2022-01-01' AND '2023-12-31' GROUP BY YEAR(o.Order_Date) ORDER BY year;""",
    """SELECT p.product_id, SUM(p.Quantity * p.Sale_price) AS total_revenue FROM retail_order.product_details p GROUP BY p.product_id ORDER BY total_revenue DESC LIMIT 10;""",
    """SELECT p.product_id, SUM(p.Quantity * p.Sale_price) AS total_revenue FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id WHERE o.Order_Date BETWEEN '2022-01-01' AND '2023-12-31' GROUP BY p.product_id ORDER BY total_revenue DESC LIMIT 10;""",
    """SELECT MONTH(o.Order_Date) AS month, YEAR(o.Order_Date) AS year, SUM(p.Sale_price) AS total_sales, LAG(SUM(p.Sale_price)) OVER (PARTITION BY MONTH(o.Order_Date) ORDER BY YEAR(o.Order_Date)) AS last_year_sales, IFNULL(ROUND((SUM(p.Sale_price) - LAG(SUM(p.Sale_price)) OVER (PARTITION BY MONTH(o.Order_Date) ORDER BY YEAR(o.Order_Date))) / LAG(SUM(p.Sale_price)) OVER (PARTITION BY MONTH(o.Order_Date) ORDER BY YEAR(o.Order_Date)) * 100, 2), 0) AS yoy_growth_percentage FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id where o.Order_Date BETWEEN '2022-01-01' AND '2023-12-31' GROUP BY YEAR(o.Order_Date), MONTH(o.Order_Date) ORDER BY month, year;""",
    """SELECT p.product_id, SUM(p.Sale_price) AS total_revenue, SUM(p.Sale_price) - SUM(p.Quantity * p.Cost_price) AS total_profit, SUM(p.Sale_price) / SUM(p.Sale_price) - 1 AS profit_margin, CASE WHEN (SUM(p.Sale_price) / SUM(p.Sale_price) - 1) > 0.2 THEN 'High Margin' WHEN (SUM(p.Sale_price) / SUM(p.Sale_price) - 1) BETWEEN 0.1 AND 0.2 THEN 'Medium Margin' ELSE 'Low Margin' END AS margin_category, ROW_NUMBER() OVER (ORDER BY SUM(p.Sale_price) DESC) AS product_rank FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id GROUP BY p.product_id HAVING total_revenue > 10000 ORDER BY product_rank;""",
    """SELECT o.Region, SUM(p.Sale_price) AS total_revenue, COUNT(o.Order_Id) AS total_sales, AVG(p.Sale_price) AS average_sale_value FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id GROUP BY o.Region ORDER BY total_revenue DESC;""",
    """SELECT o.Region, SUM(p.Sale_price) AS total_revenue, COUNT(o.Order_Id) AS total_sales, AVG(p.Sale_price) AS average_sale_value FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id WHERE o.Order_Date BETWEEN '2023-01-01' AND '2023-12-31' GROUP BY o.Region ORDER BY total_revenue DESC;""",
    """SELECT p.Product_id, p.discount_percent, SUM(p.Quantity * p.Sale_price) AS total_discounted_revenue, SUM(p.Quantity * p.List_price) AS total_original_revenue, SUM(p.Quantity * (p.List_price - p.Sale_price)) AS revenue_loss_due_to_discount, ROUND(SUM(p.Quantity * (p.List_price - p.Sale_price)) / SUM(p.Quantity * p.List_price) * 100, 2) AS discount_impact_percentage FROM retail_order.product_details p WHERE p.discount_percent > 4 GROUP BY p.product_id, p.discount_percent ORDER BY total_discounted_revenue DESC;""",
    """SELECT p.Product_id, p.discount_percent, SUM(p.Quantity * p.Sale_price) AS total_discounted_revenue, SUM(p.Quantity * p.List_price) AS total_original_revenue, SUM(p.Quantity * (p.List_price - p.Sale_price)) AS revenue_loss_due_to_discount, ROUND(SUM(p.Quantity * (p.List_price - p.Sale_price)) / SUM(p.Quantity * p.List_price) * 100, 2) AS discount_impact_percentage FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id WHERE o.Order_Date BETWEEN '2023-06-01' AND '2023-12-31' and p.discount_percent > 4 GROUP BY p.product_id, p.discount_percent ORDER BY total_discounted_revenue DESC;""",
    """SELECT YEAR(o.Order_Date) AS sale_year, MONTH(o.Order_Date) AS sale_month, COUNT(o.Order_Id) AS total_sales FROM retail_order.order_details o GROUP BY sale_year, sale_month ORDER BY sale_year DESC, sale_month DESC;""",
    """SELECT p.product_id, SUM(p.Quantity * P.Sale_price) AS total_revenue FROM retail_order.product_details p join retail_order.order_details o on p.order_id = o.order_id WHERE YEAR(o.Order_Date) = 2023 AND MONTH(o.Order_Date) = 5 GROUP BY p.product_id ORDER BY total_revenue DESC LIMIT 1;"""
    # Add more queries (total 20 queries)
]

questions = {
    queries[0]: "1.Find top 10 highest revenue generating products",
    queries[1]: "2.Find the top 5 cities with the highest profit margins",
    queries[2]: "3.Calculate the total discount given for each category",
    queries[3]: "4.Find the average sale price per product category",
    queries[4]: "5.Find the region with the highest average sale price",
    queries[5]: "6.Find the total profit per category",
    queries[6]: "7.Identify the top 3 segments with the highest quantity of orders",
    queries[7]: "8.Determine the average discount percentage given per region",
    queries[8]: "9.Find the product category with the highest total profit",
    queries[9]: "10.Calculate the total revenue generated per year",
    queries[10]: "11.Find top 10 total revenue of each product_id",
    queries[11]: "12.Find top 10 total revenue of each product_id for last 2 years",
    queries[12]: "13.Calculate the total sales and YoY growth percentage from the previous year of order_date",
    queries[13]: "14.Calculate the total_revenue, total_profit, profit_margin, margin_category and product_rank for the products having revenue above 10000",
    queries[14]: "15.Find the region with total_revenue, total_sales and average_sale_value",
    queries[15]: "16.Find the region with total_revenue, total_sales and average_sale_value from the previous year",
    queries[16]: "17.calculate the total_discounted_revenue, total_original_revenue, revenue_loss_due_to_discount, discount_impact_percentage for the product_id having discount_percent above 4 percentage",
    queries[17]: "18.calculate the total_discounted_revenue, total_original_revenue, revenue_loss_due_to_discount, discount_impact_percentage for the product_id having discount_percent above 4 percentage from the last 6 months",
    queries[18]: "19.identify the sales count by month and year of the order_date",
    queries[19]: "20.identify the top product_id having maximum revenue in a specific month",
    # Add corresponding questions for other queries here
}

# Streamlit UI
def main():
    st.title("Retail Order Data Analysis")
    
    # Dropdown to select question (query)
    selected_question = st.selectbox("Select a question to fetch data", list(questions.values()))
    
    # Get the corresponding query for the selected question
    selected_query = [query for query, question in questions.items() if question == selected_question][0]
    
    # Display the associated query
    st.write("**SQL Query:**", selected_query)
    
    # Fetch data from the selected query
    if selected_query:
        data = fetch_data_from_query(selected_query)
        
        # Display the results as a table
        if not data.empty:
            st.write("Query Results:")
            st.dataframe(data)
        else:
            st.write("No data returned for this query.")

# Run the Streamlit app
if __name__ == "__main__":
    main()
