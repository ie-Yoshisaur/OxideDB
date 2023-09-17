test:
	@success_count=0; \
	total_count=0; \
	successful_tests=""; \
	for file in $$(ls src/tests/*.rs); do \
		test_name=$$(basename $$file .rs); \
		total_count=$$((total_count + 1)); \
		echo ""; \
		echo "  Running test $$test_name..."; \
    if RUSTFLAGS="-Awarnings" cargo test $$test_name -- --nocapture; then \
			echo "  Test $$test_name succeeded."; \
			success_count=$$((success_count + 1)); \
			successful_tests="$$successful_tests\n  $$test_name"; \
		else \
			echo "  Test $$test_name failed."; \
		fi \
	done; \
	echo ""; \
	echo "Total tests: $$total_count"; \
	echo "Successful tests: $$success_count"; \
	echo "Successful test names:$$successful_tests"; \
	if [ $$success_count -ne $$total_count ]; then \
		exit 1; \
	fi

.PHONY: test
