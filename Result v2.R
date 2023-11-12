# Load necessary packages
library(stringdist)
library(dplyr)
library(tidyr)

# Assuming Product_Catalog and Product_Name_from_PoS_Transactions are data frames
# with 'Product SKU', 'Brand', 'Type', and 'Formula' columns, respectively

# Extract the 'Product SKU', 'Brand', 'Type', and 'Formula' columns
A <- data.frame(strings_A_raw = Product_Catalog$`Product SKU`, 
                Brand = Product_Catalog$Brand,
                Type = Product_Catalog$Type,
                Formula = Product_Catalog$Formula)

B <- data.frame(strings_B_raw = finalproductname$product_name,
                product_id = finalproductname$product_id)


# Function to preprocess strings
preprocess_string <- function(strings) {
  strings <- tolower(strings)
  strings <- gsub("[^[:alnum:]]", "", strings)
  return(strings)
}

# Function to find the best match in A for each string in B with consideration of Brand
find_best_match <- function(b_string_raw, b_product_id, A_strings_raw, A_brands, A_types, A_formulas) {
  # Preprocess B_string_raw
  b_string <- preprocess_string(b_string_raw)
  
  # Filter A based on Brand
  matching_brands <- A_brands[b_string %in% tolower(A_brands)]
  
  if (length(matching_brands) == 0) {
    A_filtered <- A
  } else {
    A_filtered <- A[A$Brand %in% matching_brands, ]
  }
  
  # Calculate similarity for the filtered A
  similarities <- stringdist::stringdistmatrix(b_string, preprocess_string(A_filtered$strings_A_raw), method = "lv")
  
  # Find the best match
  best_match <- A_filtered$strings_A_raw[which.min(similarities)]
  
  # Extract corresponding Brand, Type, and Formula for the best match
  best_brand <- A_filtered$Brand[which.min(similarities)]
  best_type <- A_filtered$Type[which.min(similarities)]
  best_formula <- A_filtered$Formula[which.min(similarities)]
  
  # Preprocess Best_A_strings_raw
  best_a_strings_raw <- A_strings_raw[which(A_filtered$strings_A_raw == best_match)]
  best_a_strings <- preprocess_string(best_a_strings_raw)
  
  similarity_score <- 1 - min(similarities) / max(nchar(b_string), nchar(best_match))
  return(data.frame(B_product_id = b_product_id,
                    B_string_raw = b_string_raw,
                    B_string = b_string, 
                    string_similarity = similarity_score, 
                    Best_A_strings_raw = best_a_strings_raw,
                    Best_A_strings = best_a_strings,
                    Best_A_Brand = best_brand,
                    Best_A_Type = best_type,
                    Best_A_Formula = best_formula))
}

# Apply the function to each row of B
result <- B %>%
  rowwise() %>%
  do(find_best_match(.$strings_B_raw, .$product_id, A$strings_A_raw, A$Brand, A$Type, A$Formula))

# Unnest the result, taking into account that Best_A_Brand might not exist in every row
result <- result %>%
  unnest(cols = c(B_product_id, B_string_raw, B_string, string_similarity, Best_A_strings_raw, Best_A_strings, Best_A_Brand, Best_A_Type, Best_A_Formula), keep_empty = TRUE) %>%
  filter(!is.na(B_product_id)) %>%
  distinct(B_product_id, .keep_all = TRUE)

# Order the result based on highest similarity
result <- result %>% arrange(desc(string_similarity))

# Export the result to a CSV file
write.csv(result, "result.csv", row.names = FALSE)