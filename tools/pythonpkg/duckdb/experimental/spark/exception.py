class ContributionsAcceptedError(NotImplementedError):
    """
    This method is not planned to be implemented, if you would like to implement this method
    or show your interest in this method to other members of the community,
    feel free to open up a PR or a Discussion over on https://github.com/duckdb/duckdb
    """

    def __init__(self, message=None):
        doc = self.__class__.__doc__
        if message:
            doc = message + '\n' + doc
        super().__init__(doc)


__all__ = ["ContributionsAcceptedError"]
