import timeit
import re

regex = r".*?(?<!^/home)((/run\d*)|(/dos)|(/sc)|(/[Ii]ni)|(/[Ff]in)|(/wav))"
compiled_regex = re.compile(regex)

def exclude_regex_original(job_dir: str) -> bool:
    return bool(re.match(regex, job_dir))

def exclude_regex_optimized(job_dir: str) -> bool:
    return bool(compiled_regex.match(job_dir))

test_cases = [
    "/home/user/project",
    "/data/run1",
    "/home/data/sc",
    "/home/data/Ini",
    "/other/dos",
    "/home/run2",
    "/var/log/test",
    "/home/wav"
]

def run_original():
    for tc in test_cases:
        exclude_regex_original(tc)

def run_optimized():
    for tc in test_cases:
        exclude_regex_optimized(tc)

if __name__ == "__main__":
    original_time = timeit.timeit(run_original, number=100000)
    optimized_time = timeit.timeit(run_optimized, number=100000)

    print(f"Original: {original_time:.4f} seconds")
    print(f"Optimized: {optimized_time:.4f} seconds")
    if original_time > 0:
        improvement = ((original_time - optimized_time) / original_time) * 100
        print(f"Improvement: {improvement:.2f}%")
