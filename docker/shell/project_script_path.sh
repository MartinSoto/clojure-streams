# Add bin directory in project path to global PATH.
if [ ! -z "$PROJECT_DIR" ]; then
    PATH=$PROJECT_DIR/bin:$PATH
fi
