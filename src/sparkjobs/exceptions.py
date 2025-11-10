class SparkJobsError(Exception):
    """Base exception for sparkjobs package."""

class InvalidInputError(SparkJobsError):
    """Raised when inputs are invalid or missing."""
