import test.dedupe.bin.chunkstore_test as case_test


if __name__ == '__main__':
    case_test.compress = True
    case_test.decompress = True
    case_test.async_compress = False
    case_test.write_delay = True
    case_test.read_delay = True
    case_test.decompress_speed = 241.17
    case_test.start_test(True, True)