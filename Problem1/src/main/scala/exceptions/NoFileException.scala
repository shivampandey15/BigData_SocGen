package exceptions

class NoFileException(message: String) extends Exception{
  def this(message: String, cause: Throwable) = {
    this(message)
    initCause(cause)
  }
  def this(cause: Throwable) = {
    this(Option(cause).map(_.toString()).orNull)
    initCause(cause)
  }
  def this() = {
    this(null: String)
  }
}
