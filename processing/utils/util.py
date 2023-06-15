def sum_two_dicts(dict1, dict2):
    result_dict = {}

    # Combine keys from both dictionaries
    all_keys = set(dict1.keys()) | set(dict2.keys())

    for key in all_keys:
        result_dict[key] = {}

        if key in dict1:
            result_dict[key].update(dict1[key])

        if key in dict2:
            for subkey in dict2[key]:
                if subkey in result_dict[key]:
                    result_dict[key][subkey] += dict2[key][subkey]
                else:
                    result_dict[key][subkey] = dict2[key][subkey]
                    
    return result_dict


def sum_dicts(list_dicts):
    for n in range(len(list_dicts)-1):
        if n == 0:
                result_dict = sum_two_dicts(list_dicts[n], list_dicts[n+1])
        else:
                result_dict = sum_two_dicts(result_dict, list_dicts[n+1])
                
    return result_dict


def sort_dict(my_dict):
    # Sort secondary keys in each sub-dictionary
    for key in my_dict:
        my_dict[key] = dict(sorted(my_dict[key].items(), key=lambda x: x[1]))

    # Sort primary keys by the sum of secondary key values
    my_dict = dict(sorted(my_dict.items(), key=lambda x: sum(x[1].values())))
    
    return my_dict