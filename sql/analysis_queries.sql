-- 1. KPI Summary
SELECT 
    COUNT(DISTINCT Order_ID) as total_deliveries,
    COUNT(DISTINCT CAST(Order_Date AS DATE)) as active_days,
    ROUND(AVG(Delivery_Time), 2) as avg_delivery_time_min,
    ROUND(AVG(Agent_Rating), 2) as avg_agent_rating,
    COUNT(DISTINCT Area) as areas_covered
FROM deliveries;

-- 2. Deliveries by Day of Week
SELECT 
    order_day_name,
    COUNT(*) as delivery_count,
    ROUND(AVG(Delivery_Time), 2) as avg_delivery_time
FROM deliveries
GROUP BY order_day_name
ORDER BY order_day_of_week;

-- 3. Deliveries by Area
SELECT 
    Area,
    COUNT(*) as delivery_count,
    ROUND(AVG(Delivery_Time), 2) as avg_delivery_time,
    ROUND(AVG(Agent_Rating), 2) as avg_rating
FROM deliveries
GROUP BY Area
ORDER BY delivery_count DESC;

-- 4. Weather Impact
SELECT 
    Weather,
    COUNT(*) as delivery_count,
    ROUND(AVG(Delivery_Time), 2) as avg_delivery_time
FROM deliveries
GROUP BY Weather
ORDER BY delivery_count DESC;

-- 5. Top Categories
SELECT 
    Category,
    COUNT(*) as order_count,
    ROUND(AVG(Delivery_Time), 2) as avg_delivery_time
FROM deliveries
GROUP BY Category
ORDER BY order_count DESC
LIMIT 10;
