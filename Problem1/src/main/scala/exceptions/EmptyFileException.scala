package exceptions

class EmptyFileException(message: String) extends Exception(message: String) {
  def this(message: String, cause: Throwable) = {
    this(message)
    initCause(cause)
  }
}
