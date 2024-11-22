# output_generator.py
def write_to_file(filename, data):
    flat_data = [str(item) for sublist in data for item in (sublist if isinstance(sublist, list) else [sublist])]

    # ... code to write data to a file
    file_content = ''.join(flat_data)
    # file_content = ''.join(data)
    with open(filename, 'a') as file:
        file.write(file_content)