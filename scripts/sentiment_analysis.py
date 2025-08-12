import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, length
from pyspark.sql.types import FloatType, StringType
from textblob import TextBlob
import re

def clean_tweet_text(text):
    """Clean tweet text by removing URLs, mentions, and extra spaces"""
    if not text:
        return ""
    
    # Convert to string in case it's not
    text = str(text)
    
    # Remove URLs
    text = re.sub(r'https?://\S+', '', text)
    text = re.sub(r'www\.\S+', '', text)
    
    # Remove extra whitespace
    text = ' '.join(text.split())
    
    return text.strip()

def get_sentiment_score(text):
    """Calculate sentiment polarity score using TextBlob"""
    try:
        if not text:
            return 0.0
        
        # Clean the text first
        cleaned_text = clean_tweet_text(text)
        
        # Skip if the cleaned text is too short
        if len(cleaned_text) < 5:
            return 0.0
        
        blob = TextBlob(cleaned_text)
        return float(blob.sentiment.polarity)
    except Exception as e:
        print(f"Error in sentiment analysis: {e}")
        return 0.0

def get_sentiment_label(score):
    """Convert sentiment score to label with adjusted thresholds"""
    if score > 0.1:
        return "positive"
    elif score < -0.1:
        return "negative"
    else:
        return "neutral"

def get_subjectivity_score(text):
    """Calculate subjectivity score"""
    try:
        if not text:
            return 0.5  # Default to middle value
            
        cleaned_text = clean_tweet_text(text)
        if len(cleaned_text) < 5:
            return 0.5
            
        blob = TextBlob(cleaned_text)
        return float(blob.sentiment.subjectivity)
    except:
        return 0.5

def analyze_tweets(input_path, output_path):
    """Main function to analyze tweet sentiments"""
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("TwitterSentimentAnalysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Set log level to reduce output
    spark.sparkContext.setLogLevel("ERROR")
    
    print(f"Reading data from: {input_path}")
    
    # Read the complete CSV file
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Create UDFs (User Defined Functions)
    clean_text_udf = udf(clean_tweet_text, StringType())
    sentiment_score_udf = udf(get_sentiment_score, FloatType())
    sentiment_label_udf = udf(get_sentiment_label, StringType())
    subjectivity_udf = udf(get_subjectivity_score, FloatType())
    
    # Apply text cleaning and sentiment analysis
    df_with_sentiment = df \
        .withColumn("cleaned_content", clean_text_udf(col("content"))) \
        .withColumn("content_length", length(col("cleaned_content"))) \
        .withColumn("sentiment_score", sentiment_score_udf(col("content"))) \
        .withColumn("sentiment_label", sentiment_label_udf(col("sentiment_score"))) \
        .withColumn("subjectivity", subjectivity_udf(col("content")))
    
    # Filter out tweets that are too short after cleaning (if any)
    df_with_sentiment = df_with_sentiment.filter(col("content_length") >= 5)
    
    # Select columns for output
    output_columns = ["author", "content", "cleaned_content", "sentiment_score", 
                      "sentiment_label", "subjectivity", "content_length",
                      "date_time", "language", "country",
                      "number_of_likes", "number_of_shares", "id"]
    
    # Filter columns that exist
    existing_columns = [col for col in output_columns if col in df_with_sentiment.columns]
    
    # Save the main detailed results
    df_with_sentiment.select(*existing_columns) \
        .coalesce(1) \
        .write.mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)
    
    print(f"Analysis complete. Results saved to: {output_path}")
    
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze sentiment of tweets')
    parser.add_argument('--input_path', required=True, 
                        help='S3 path to input CSV file')
    parser.add_argument('--output_path', required=True, 
                        help='S3 path for output results')
    
    args = parser.parse_args()
    analyze_tweets(args.input_path, args.output_path)
