# Add bin directory in project path to global PATH.
if [ ! -z "$PROJECT_DIR" ]; then
    PATH=$PROJECT_DIR/bin:$PATH

    HISTFILE=$PROJECT_DIR/.bash_history
    HISTFILESIZE=1000
    HISTSIZE=1000
    HISTCONTROL=ignoredups
fi
