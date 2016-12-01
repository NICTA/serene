package com.nicta.dataint.ingestion.loader

trait FileLoaderTrait[A] extends LoaderTrait[A] {
	def load(filePath: String): A
}