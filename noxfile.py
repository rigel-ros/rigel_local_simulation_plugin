import noxfile


# Run unit tests and perform test coverage.
@noxfile.session(python=["3.8", "3.9"])
def tests(session: noxfile.sessions.Session) -> None:
    session.install("poetry")
    session.run("poetry", "install")
    session.run("coverage", "run", "-m", "pytest")
    session.run("coverage", "report")


# Run flake8 linter.
@noxfile.session
def lint(session: noxfile.sessions.Session) -> None:
    session.install("poetry")
    session.run("poetry", "install")
    session.run("flake8", ".")


# # Run mypy type checker.
@noxfile.session
def typing(session: noxfile.sessions.Session) -> None:
    session.install("poetry")
    session.run("poetry", "install")
    session.run("mypy", ".")
