import redis

def check_redis_keys():
    try:
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        keys = r.keys("*")
        if keys:
            print("Keys found in Redis:")
            for key in keys:
                print(key)
        else:
            print("No keys found in Redis.")
    except redis.exceptions.ConnectionError as e:
        print(f"Could not connect to Redis: {e}")

if __name__ == "__main__":
    check_redis_keys()
