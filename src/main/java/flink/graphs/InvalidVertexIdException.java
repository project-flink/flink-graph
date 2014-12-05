package flink.graphs;

import org.apache.flink.api.common.InvalidProgramException;

/**
 * A special case of the {@link InvalidProgramException}, indicating that one or more
 * vertex Ids in the edge set input are not present in the vertex set input.
 */
public class InvalidVertexIdException extends InvalidProgramException {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new exception with no message.
	 */
	public InvalidVertexIdException() {
		super();
	}

	/**
	 * Creates a new exception with the given message.
	 * 
	 * @param message The exception message.
	 */
	public InvalidVertexIdException(String message) {
		super(message);
	}
	
	public InvalidVertexIdException(String message, Throwable e) {
		super(message, e);
	}
}
