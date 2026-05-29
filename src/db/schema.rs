use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helper macro: enum stored as TEXT in PostgreSQL
//
// Generates: Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::Type (manual),
//            and a `variants()` helper.
// ---------------------------------------------------------------------------
macro_rules! impl_text_enum {
    ($name:ident { $($variant:ident),+ $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub fn variants() -> &'static [Self] {
                &[$(Self::$variant),+]
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let s = serde_json::to_string(self)
                    .unwrap_or_default()
                    .trim_matches('"')
                    .to_string();
                write!(f, "{}", s)
            }
        }

        impl sqlx::Type<sqlx::Postgres> for $name {
            fn type_info() -> sqlx::postgres::PgTypeInfo {
                <String as sqlx::Type<sqlx::Postgres>>::type_info()
            }
        }

        impl<'q> sqlx::Encode<'q, sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut sqlx::postgres::PgArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                let s = serde_json::to_string(self)
                    .unwrap_or_default()
                    .trim_matches('"')
                    .to_string();
                <String as sqlx::Encode<'q, sqlx::Postgres>>::encode(s, buf)
            }
        }

        impl<'r> sqlx::Decode<'r, sqlx::Postgres> for $name {
            fn decode(
                value: sqlx::postgres::PgValueRef<'r>,
            ) -> Result<Self, sqlx::error::BoxDynError> {
                let s = <String as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
                serde_json::from_str(&format!("\"{}\"", s)).map_err(Into::into)
            }
        }
    };
}

// ============================= Enums TEXT ====================================

impl_text_enum!(FeedFetchStatus {
    Pending,
    Fetching,
    Success,
    Failed,
    Disabled,
});

impl_text_enum!(QualityStatus {
    Pending,
    Qualified,
    Rejected,
    PendingQualification,
});

impl_text_enum!(DuplicateStatus {
    Pending,
    Distinct,
    Duplicate,
    NearDuplicate,
});

impl_text_enum!(ProcessingStatus {
    Ingested,
    Extracted,
    ExtractionFailed,
    PendingQualification,
    Qualified,
    Rejected,
});

impl_text_enum!(RunStatus {
    Running,
    Completed,
    Failed,
});

impl_text_enum!(RunTriggerType {
    Scheduled,
    Manual,
    Test,
});

impl_text_enum!(StepName {
    Ingest,
    Content,
    Qualification,
    Audio,
});

impl_text_enum!(StepStatus {
    Running,
    Completed,
    Failed,
});

// ============================= Enum PostgreSQL ===============================
// job_status est un vrai ENUM PostgreSQL → derive sqlx::Type

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "job_status", rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Dead,
}

// ============================= Structs =======================================

#[derive(Debug, Clone, FromRow)]
pub struct FeedSource {
    pub id: String,
    pub feed_url: String,
    pub name: String,
    pub category: Option<String>,
    pub description: Option<String>,
    pub logo: Option<String>,
    pub priority: i32,
    pub tier: Option<String>,
    pub fetch_status: FeedFetchStatus,
    pub last_fetch_error: Option<String>,
    pub last_fetch_at: Option<DateTime<Utc>>,
    pub last_ingested_pub_date: Option<DateTime<Utc>>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow)]
pub struct RawArticle {
    pub id: Uuid,
    pub source_id: String,
    pub title: String,
    pub url: String,
    pub description: Option<String>,
    pub image_url: Option<String>,
    pub author: Option<String>,
    pub pub_date: Option<DateTime<Utc>>,
    pub content: Option<String>,
    pub content_length: Option<i32>,
    pub content_hash: Option<String>,
    pub title_clean: Option<String>,
    pub canonical_url: Option<String>,
    pub processing_status: ProcessingStatus,
    pub quality_status: QualityStatus,
    pub duplicate_status: DuplicateStatus,
    pub duplicate_of: Option<Uuid>,
    pub preferred_extraction_method: Option<String>,
    pub extraction_attempts: i32,
    pub last_extraction_error: Option<String>,
    pub last_extraction_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow)]
pub struct PipelineRun {
    pub id: Uuid,
    pub status: RunStatus,
    pub trigger_type: RunTriggerType,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    pub feeds_count: Option<i32>,
    pub articles_ingested: i32,
    pub articles_qualified: i32,
    pub articles_rejected: i32,
    pub articles_duplicate: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow)]
pub struct PipelineStepRun {
    pub id: Uuid,
    pub run_id: Uuid,
    pub step_name: StepName,
    pub status: StepStatus,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    pub items_count: i32,
    pub items_processed: i32,
    pub items_failed: i32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow)]
pub struct RejectedArticle {
    pub id: Uuid,
    pub article_id: Uuid,
    pub source_id: String,
    pub title: String,
    pub url: String,
    pub reason: String,
    pub details: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow)]
pub struct Job {
    pub id: Uuid,
    pub job_type: String,
    pub payload: serde_json::Value,
    pub status: JobStatus,
    pub priority: i32,
    pub attempts: i32,
    pub max_attempts: i32,
    pub run_at: DateTime<Utc>,
    pub locked_at: Option<DateTime<Utc>>,
    pub locked_by: Option<String>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
