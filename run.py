import scrape
from includes.debug import *



# found_records = []
# max_total_records_per_shard = 4
# shard_id = 123
#
# outer_response = [
# {"Records": [1,2,3]},
# {"Records": [4,5,6]},
# {"Records": [7,8,9]},
# ]
#
# i = 0
# while i < len((outer_response)):
#     if i > 0:
#         pvdd(found_records)
#
#     response = outer_response[i]
#     for record in response["Records"]:
#         pvd(found_records)
#         # print(f'\n\nfound_record count: {len(found_records)}\n')
#         if len(found_records) > max_total_records_per_shard - 1:
#             # print(f'Reached {max_total_records_per_shard} max records per shard limit for shard {shard_id}')
#             break
#         found_records.append(record)
#     i += 1
# pvdd(found_records)
#




# if __name__ == "__main__":
#     scrape.main()
