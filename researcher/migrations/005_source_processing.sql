ALTER TABLE document_chunks ADD COLUMN processing_state TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE ingestion_queue ADD COLUMN extraction_state TEXT NOT NULL DEFAULT 'pending';
CREATE INDEX chunk_processing ON document_chunks(processing_state,version_id);
