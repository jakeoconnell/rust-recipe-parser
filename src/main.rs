use csv::Reader;
use neo4rs::*;
use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json;
use std::error::Error;
use std::{fmt, string};

#[derive(Debug, Serialize, Deserialize)]
struct Recipe {
    id: i32,
    name: String,
    description: String,
    #[serde(deserialize_with = "deserialize_string_array")]
    ingredients: Vec<String>,
    minutes: i32,
    #[serde(deserialize_with = "deserialize_string_array")]
    steps: Vec<String>,
    #[serde(deserialize_with = "deserialize_float_array")]
    nutrition: Vec<f32>,
}

fn deserialize_string_array<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserializer.deserialize_string(StringArrayVisitor)
}

struct StringArrayVisitor;

impl<'de> serde::de::Visitor<'de> for StringArrayVisitor {
    type Value = Vec<String>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let parts = value.trim_matches(|c| c == '[' || c == ']').split(',');
        let mut strings = Vec::new();
        for part in parts {
            // Remove single quotes and extra whitespace
            let string = part.trim().trim_matches(|c| c == '\'' || c == '"');
            strings.push(string.to_string());
        }

        Ok(strings)
    }
}

fn deserialize_float_array<'de, D>(deserializer: D) -> Result<Vec<f32>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_string(FloatArrayVisitor)
}

struct FloatArrayVisitor;

impl<'de> Visitor<'de> for FloatArrayVisitor {
    type Value = Vec<f32>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string representing an array of floats")
    }

    fn visit_str<E>(self, value: &str) -> Result<Vec<f32>, E>
    where
        E: de::Error,
    {
        // Trim the brackets and then split the string by comma
        let parts = value.trim_matches(|c| c == '[' || c == ']').split(',');

        let mut floats = Vec::new();
        for part in parts {
            match part.trim().parse() {
                Ok(num) => floats.push(num),
                Err(_) => return Err(E::custom("failed to parse float")),
            }
        }
        Ok(floats)
    }
}

async fn add_ingredients_to_recipe(
    graph: &Graph,
    recipe_id: i32,
    ingredients: Vec<String>,
) -> Result<(), Box<dyn Error>> {
    let mut tx = graph.start_txn().await?;

    for ingredient in ingredients {
        // Create ingredient node if it doesn't exist
        let query = Query::new("MERGE (i:Ingredient {name: $name})".to_string())
            .param("name", ingredient.clone());
        tx.run(query).await?;

        // Create relationship between recipe and ingredient
        let rel_query = Query::new("MATCH (r:Recipe {id: $recipe_id}), (i:Ingredient {name: $ingredient_name}) MERGE (r)-[:CONTAINS]->(i)".to_string())
            .param("recipe_id", recipe_id)
            .param("ingredient_name", ingredient);
        tx.run(rel_query).await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn add_recipe_to_neo4j(
    graph: &Graph,
    recipe: &Recipe,
) -> Result<(), Box<dyn std::error::Error>> {
    let query = Query::new("CREATE (r:Recipe {id: $id, name: $name, description: $description, minutes: $minutes, nutrition: $nutrition, steps: $steps}) RETURN r".to_string())
        .param("id", recipe.id)
        .param("name", recipe.name.clone())
        .param("description", recipe.description.clone())
        .param("minutes", recipe.minutes)
        .param("nutrition", recipe.nutrition.clone())
        .param("steps", recipe.steps.clone());

    let mut tx = graph.start_txn().await?;
    let _results = tx.run(query).await?;
    tx.commit().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let graph = Graph::new("bolt://10.144.2.189:7687", "neo4j", "HAHAHA").await?;

    let mut rdr = Reader::from_path("data/RAW_recipes.csv")?;

    for result in rdr.deserialize() {
        let recipe: Recipe = result?;
        // println!("{:?}", recipe);
        // let json = serde_json::to_string_pretty(&recipe)?;
        // println!("{}", json);
        add_recipe_to_neo4j(&graph, &recipe).await?;

        let recipe_id: i32 = recipe.id.clone();
        let ingredients = recipe.ingredients.clone();

        add_ingredients_to_recipe(&graph, recipe_id, ingredients).await?;
    }

    Ok(())
}
