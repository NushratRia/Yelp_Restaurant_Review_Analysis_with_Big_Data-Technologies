from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RestaurantReviews") \
    .getOrCreate()

# Define file paths (use double backslashes or raw strings to avoid escape issues)
file_paths = [
    r'C:\Users\ASUS\Downloads\yelp_dataset\yelp_academic_dataset_review.json',
    r'C:\Users\ASUS\Downloads\yelp_dataset\yelp_academic_dataset_tip.json',
    r'C:\Users\ASUS\Downloads\yelp_dataset\yelp_academic_dataset_checkin.json',
    r'C:\Users\ASUS\Downloads\yelp_dataset\yelp_academic_dataset_user.json',
    r'C:\Users\ASUS\Downloads\yelp_dataset\yelp_academic_dataset_business.json'
]

# Load multiple JSON files into a single DataFrame
df_reviews = spark.read.json(file_paths)

# Show the schema of the DataFrame
df_reviews.printSchema()

# Display the first few rows of the DataFrame
df_reviews.show(truncate=False)

# Stop the Spark session when done
spark.stop()
