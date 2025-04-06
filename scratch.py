
rows_inserted = 4
initial_count = 5
final_count = 6
rows_added = 12

rows_matched_str = "Rows inserted matched DB diff"
rows_not_matched_str = print(f"Rows inserted: {rows_inserted} \n DB Rows initially: {initial_count} \n DB Rows after insertion: {final_count}\n DB Rows rows after - rows initial: {rows_added} \n Expected match between rows inserted: {rows_inserted} and rows added {rows_added}, \n actual diff = {rows_inserted-rows_added}")


print(rows_matched_str)
print(rows_not_matched_str)
