import subprocess
import sys
import os

base_dir = os.path.dirname(__file__)
all_passed = True

print("=" * 60)
print("Running API Server Tests...")
print("=" * 60)
result = subprocess.run(
    [sys.executable, os.path.join(base_dir, 'api', 'test_server.py')],
    cwd=os.path.join(base_dir, 'api')
)
if result.returncode != 0:
    all_passed = False

print("\n" + "=" * 60)
print("Running Lambda Processor Tests...")
print("=" * 60)
result = subprocess.run(
    [sys.executable, os.path.join(base_dir, 'lambda', 'processor', 'test_processor.py')],
    cwd=os.path.join(base_dir, 'lambda', 'processor')
)
if result.returncode != 0:
    all_passed = False

print("\n" + "=" * 60)
print("Running Lambda Compactor Tests...")
print("=" * 60)
result = subprocess.run(
    [sys.executable, os.path.join(base_dir, 'lambda', 'compactor', 'test_compactor.py')],
    cwd=os.path.join(base_dir, 'lambda', 'compactor')
)
if result.returncode != 0:
    all_passed = False

print("\n" + "=" * 60)
if all_passed:
    print("All tests passed!")
    sys.exit(0)
else:
    print("Some tests failed!")
    sys.exit(1)

