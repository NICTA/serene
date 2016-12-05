package au.csiro.data61.matcher.ingestion.loader

trait FileLoaderTrait[A] extends LoaderTrait[A] {
	def load(filePath: String): A
}