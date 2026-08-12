//! LLM question generation using `async-openai` (DeepSeek by default, any
//! OpenAI-compatible endpoint works). Falls back to a built-in question bank
//! on failure.

use async_openai::{
    config::OpenAIConfig,
    traits::RequestOptionsBuilder,
    types::chat::{
        ChatCompletionRequestMessage,
        ChatCompletionRequestSystemMessage,
        ChatCompletionRequestSystemMessageContent,
        ChatCompletionRequestUserMessage,
        ChatCompletionRequestUserMessageContent,
        CreateChatCompletionRequestArgs,
        ResponseFormat,
    },
    Client,
};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

/// A generated question. Choices are shuffled; `answer_index` points into
/// the shuffled list.
#[derive(Debug, Clone)]
pub struct Question {
    pub text: String,
    pub choices: Vec<String>,
    pub answer_index: usize,
}

#[derive(Clone)]
pub struct LlmClient {
    client: Client<OpenAIConfig>,
    model: String,
    has_key: bool,
    /// server-wide cache of recently asked question texts (cross-game dedupe)
    recent: Arc<RwLock<VecDeque<String>>>,
}

const RECENT_LIMIT: usize = 200;

const BATCH_SYSTEM_PROMPT: &str = "Sen bir bilgi yarışması soru üreticisisin. Kullanıcının verdiği kategori, zorluk (easy, medium, hard) ve adet bilgisine uygun sorular üret. Kategori hangi dilde verilmiş ise, sorular da o dilde olacak. Sorular BİRBİRİNDEN FARKLI konulara değinmeli; aynı temanın hafif varyasyonlarını üretme. Çıktını KESİNLİKLE sadece aşağıdaki JSON formatında üretmelisin: {\"questions\": [{\"question\": \"Soru metni\", \"answers\": [{\"text\": \"şık metni\", \"is_correct\": false}, {\"text\": \"şık metni\", \"is_correct\": true}, {\"text\": \"şık metni\", \"is_correct\": false}, {\"text\": \"şık metni\", \"is_correct\": false}]}]}. Her sorunun 'answers' alanı tam olarak 4 şık içermeli ve yalnızca bir tanesi is_correct: true olmalı.";

const SYSTEM_PROMPT: &str = "Sen bir bilgi yarışması soru üreticisisin. Kullanıcının verdiği kategori ve zorluk (easy, medium, hard) seviyesine uygun, 4 şıklı bir soru üret. Kategori hangi dilde verilmiş ise, sorular da o dilde olacak. Çıktını KESİNLİKLE sadece aşağıdaki JSON formatında üretmelisin: {\"question\": \"Soru metni\", \"answers\": [{\"text\": \"A şıkkı metni\", \"is_correct\": false}, {\"text\": \"B şıkkı metni\", \"is_correct\": true}, {\"text\": \"C şıkkı metni\", \"is_correct\": false}, {\"text\": \"D şıkkı metni\", \"is_correct\": false}]}. 'answers' tam olarak 4 şık içermeli ve yalnızca bir tanesi is_correct: true olmalı.";

impl LlmClient {
    pub fn from_env() -> Self {
        // set LLM_API_KEY in the environment — without it the mock bank is used
        let api_key = std::env::var("LLM_API_KEY").unwrap_or_default();
        let base_url = std::env::var("LLM_BASE_URL")
            .unwrap_or_else(|_| "https://api.deepseek.com".into());
        let model = std::env::var("LLM_MODEL")
            .unwrap_or_else(|_| "deepseek-v4-pro".into());

        let has_key = !api_key.is_empty();
        let config = OpenAIConfig::new()
            .with_api_base(base_url)
            .with_api_key(api_key);

        LlmClient {
            client: Client::with_config(config),
            model,
            has_key,
            recent: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    /// Recently asked question texts (shared across all rooms).
    pub fn recent_questions(&self) -> Vec<String> {
        self.recent.read().unwrap().iter().cloned().collect()
    }

    pub fn record_asked(&self, texts: impl IntoIterator<Item = String>) {
        let mut recent = self.recent.write().unwrap();
        for t in texts {
            recent.push_back(t);
        }
        while recent.len() > RECENT_LIMIT {
            recent.pop_front();
        }
    }

    /// Generate `count` distinct questions in ONE call — a single context, so
    /// the model itself prevents intra-game repeats. The server-wide recent
    /// cache guards against cross-game repeats.
    pub async fn generate_batch(
        &self,
        difficulty: &str,
        category: &str,
        count: u32,
    ) -> Result<Vec<Question>, String> {
        if !self.has_key {
            return Err("LLM_API_KEY not set".into());
        }
        let mut user = format!(
            "Kategori: {category}\nZorluk Seviyesi: {difficulty}\nAdet: {count}"
        );
        let previous = self.recent_questions();
        if !previous.is_empty() {
            let prev = previous
                .iter()
                .rev()
                .take(30)
                .cloned()
                .collect::<Vec<_>>()
                .join(" | ");
            user.push_str(&format!(
                "\nDaha önce sorulan sorular (BUNLARI VE ÇOK BENZERLERİNİ TEKRARLAMA): {prev}"
            ));
        }

        let messages = vec![
            ChatCompletionRequestMessage::from(ChatCompletionRequestSystemMessage {
                content: ChatCompletionRequestSystemMessageContent::Text(
                    BATCH_SYSTEM_PROMPT.to_string(),
                ),
                name: None,
            }),
            ChatCompletionRequestMessage::from(ChatCompletionRequestUserMessage {
                content: ChatCompletionRequestUserMessageContent::Text(user),
                name: None,
            }),
        ];

        let request = CreateChatCompletionRequestArgs::default()
            .model(&self.model)
            .messages(messages)
            .response_format(ResponseFormat::JsonObject)
            .temperature(0.8)
            .build()
            .map_err(|e| e.to_string())?;

        let response = self
            .client
            .chat()
            .path("/chat/completions")
            .map_err(|e| e.to_string())?
            .create(request)
            .await
            .map_err(|e| e.to_string())?;

        let content = response
            .choices
            .first()
            .and_then(|c| c.message.content.as_deref())
            .ok_or("empty llm response")?;

        parse_question_batch(content)
    }

    /// Never-fail batch: on total failure or shortfall, tops up with mock
    /// questions so the game always has exactly `count` questions.
    pub async fn generate_batch_or_mock(
        &self,
        difficulty: &str,
        category: &str,
        count: u32,
    ) -> Vec<Question> {
        let mut questions = match self.generate_batch(difficulty, category, count).await {
            Ok(q) => q,
            Err(e) => {
                tracing::warn!("llm batch generation failed ({e}), using mock questions");
                Vec::new()
            }
        };
        let mut seed = 0usize;
        while questions.len() < count as usize {
            let q = mock_question(seed);
            seed += 1;
            if !questions.iter().any(|existing| existing.text == q.text) {
                questions.push(q);
            }
        }
        questions.truncate(count as usize);
        questions
    }

    /// Generate one question. `previous` holds recently asked question texts
    /// so the model avoids repeating itself.
    pub async fn generate(
        &self,
        difficulty: &str,
        category: &str,
        previous: &[String],
    ) -> Result<Question, String> {
        let mut user = format!("Kategori: {category}\nZorluk Seviyesi: {difficulty}");
        if !previous.is_empty() {
            let prev = previous
                .iter()
                .rev()
                .take(6)
                .cloned()
                .collect::<Vec<_>>()
                .join(" | ");
            user.push_str(&format!(
                "\nDaha önce sorulan sorular (BUNLARI TEKRARLAMA): {prev}"
            ));
        }

        let messages = vec![
            ChatCompletionRequestMessage::from(ChatCompletionRequestSystemMessage {
                content: ChatCompletionRequestSystemMessageContent::Text(
                    SYSTEM_PROMPT.to_string(),
                ),
                name: None,
            }),
            ChatCompletionRequestMessage::from(ChatCompletionRequestUserMessage {
                content: ChatCompletionRequestUserMessageContent::Text(user),
                name: None,
            }),
        ];

        let request = CreateChatCompletionRequestArgs::default()
            .model(&self.model)
            .messages(messages)
            .response_format(ResponseFormat::JsonObject)
            .temperature(0.7)
            .build()
            .map_err(|e| e.to_string())?;

        // DeepSeek does NOT use /v1 prefix — override the default path.
        let response = self
            .client
            .chat()
            .path("/chat/completions")
            .map_err(|e| e.to_string())?
            .create(request)
            .await
            .map_err(|e| e.to_string())?;

        let content = response
            .choices
            .first()
            .and_then(|c| c.message.content.as_deref())
            .ok_or("empty llm response")?;

        parse_question(content)
    }

    /// Never-fail variant used by the game loop: falls back to the mock bank.
    pub async fn generate_or_mock(&self, difficulty: &str, category: &str, previous: &[String]) -> Question {
        match self.generate(difficulty, category, previous).await {
            Ok(q) => q,
            Err(e) => {
                tracing::warn!("llm generation failed ({e}), using mock question");
                let _ = (difficulty, category);
                mock_question(previous.len())
            }
        }
    }
}

fn parse_question_batch(content: &str) -> Result<Vec<Question>, String> {
    let cleaned = strip_fences(content);
    let v: Value = serde_json::from_str(cleaned).map_err(|e| e.to_string())?;
    let questions = v["questions"].as_array().ok_or("missing questions array")?;
    if questions.is_empty() {
        return Err("empty questions array".into());
    }
    questions.iter().map(parse_question_value).collect()
}

fn strip_fences(content: &str) -> &str {
    content
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim()
}

fn parse_question(content: &str) -> std::result::Result<Question, String> {
    let v: Value = serde_json::from_str(strip_fences(content)).map_err(|e| e.to_string())?;
    parse_question_value(&v)
}

fn parse_question_value(v: &Value) -> std::result::Result<Question, String> {

    let text = v["question"].as_str().ok_or("missing question")?.to_string();
    let answers = v["answers"].as_array().ok_or("missing answers")?;
    if answers.len() != 4 {
        return Err(format!("expected 4 answers, got {}", answers.len()));
    }

    let mut choices = Vec::with_capacity(4);
    let mut correct: Option<usize> = None;
    for (i, a) in answers.iter().enumerate() {
        choices.push(a["text"].as_str().ok_or("missing answer text")?.to_string());
        if a["is_correct"].as_bool().unwrap_or(false) && correct.is_none() {
            correct = Some(i);
        }
    }
    let correct = correct.ok_or("no correct answer marked")?;

    // shuffle so the correct answer isn't predictably positioned
    let mut order: Vec<usize> = vec![0, 1, 2, 3];
    let entropy = nanoid::nanoid!(8);
    let bytes = entropy.as_bytes();
    for i in (1..order.len()).rev() {
        let j = bytes[i] as usize % (i + 1);
        order.swap(i, j);
    }
    let shuffled: Vec<String> = order.iter().map(|&i| choices[i].clone()).collect();
    let answer_index = order.iter().position(|&i| i == correct).unwrap();

    Ok(Question {
        text,
        choices: shuffled,
        answer_index,
    })
}

/// Offline fallback so the demo works without an API key / on LLM failure.
pub(crate) fn mock_question(seed: usize) -> Question {
    let bank: &[(&str, [&str; 4], usize)] = &[
        ("Hangisi bir programlama dilidir?", ["Python", "HTML", "CSS", "SQL"], 0),
        ("Dünyanın en kalabalık ülkesi hangisidir?", ["Hindistan", "Çin", "ABD", "Endonezya"], 0),
        ("Işık hızı yaklaşık kaç km/s'dir?", ["300.000", "150.000", "1.000", "30.000"], 0),
        ("Hangisi bir asal sayıdır?", ["97", "91", "87", "93"], 0),
        ("Fotosentezde hangi gaz üretilir?", ["Oksijen", "Karbondioksit", "Azot", "Hidrojen"], 0),
        ("İstanbul hangi yılda fethedilmiştir?", ["1453", "1451", "1461", "1444"], 0),
        ("Hangisi en büyük gezegen?", ["Jüpiter", "Satürn", "Neptün", "Mars"], 0),
        ("DNA'nın açılımı nedir?", ["Deoksiribonükleik asit", "Ribonükleik asit", "Amino asit", "Folik asit"], 0),
    ];
    let (text, choices, correct) = bank[seed % bank.len()];
    Question {
        text: text.to_string(),
        choices: choices.iter().map(|s| s.to_string()).collect(),
        answer_index: correct,
    }
}
