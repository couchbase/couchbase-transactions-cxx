#include <vector>
#include <string>
#include <stdexcept>

#include "atr_ids.hxx"

namespace couchbase
{
namespace transactions
{
    // Each vbucket has 1 ATR.  The '#136' stuff is used purely to force a document to a particular vbucket
    const std::vector< std::string >
        ATR_IDS({ "atr-0-#32",     "atr-1-#122",     "atr-2-#194",     "atr-3-#284",     "atr-4-#9b",     "atr-5-#1285",    "atr-6-#b18",
                  "atr-7-#a08",    "atr-8-#24",      "atr-9-#8db",     "atr-10-#59b8",   "atr-11-#11b9",  "atr-12-#226",    "atr-13-#136",
                  "atr-14-#924",   "atr-15-#bbc",    "atr-16-#83c",    "atr-17-#89",     "atr-18-#835",   "atr-19-#b21",    "atr-20-#204",
                  "atr-21-#114",   "atr-22-#1ba",    "atr-23-#2ca",    "atr-24-#22",     "atr-25-#8f8",   "atr-26-#a18",    "atr-27-#b08",
                  "atr-28-#bb1",   "atr-29-#8c5",    "atr-30-#a29",    "atr-31-#b39",    "atr-32-#815",   "atr-33-#b01",    "atr-34-#287",
                  "atr-35-#197",   "atr-36-#1b0d",   "atr-37-#1b51",   "atr-38-#2a8",    "atr-39-#728",   "atr-40-#268",    "atr-41-#178",
                  "atr-42-#1086",  "atr-43-#1092",   "atr-44-#8b2",    "atr-45-#bc6",    "atr-46-#b0c",   "atr-47-#38",     "atr-48-#861",
                  "atr-49-#b3d",   "atr-50-#10ce",   "atr-51-#38",     "atr-52-#bcf",    "atr-53-#8bb",   "atr-54-#123",    "atr-55-#233",
                  "atr-56-#13d9",  "atr-57-#69f9",   "atr-58-#248",    "atr-59-#158",    "atr-60-#879",   "atr-61-#1559",   "atr-62-#ba9",
                  "atr-63-#9a",    "atr-64-#2b5",    "atr-65-#1c5",    "atr-66-#141",    "atr-67-#27",    "atr-68-#458",    "atr-69-#1f8",
                  "atr-70-#13a",   "atr-71-#c9",     "atr-72-#38c8",   "atr-73-#1529",   "atr-74-#9b",    "atr-75-#ba8",    "atr-76-#89e",
                  "atr-77-#b8a",   "atr-78-#893",    "atr-79-#b87",    "atr-80-#5868",   "atr-81-#1069",  "atr-82-#a3",     "atr-83-#3d8",
                  "atr-84-#824",   "atr-85-#b30",    "atr-86-#30",     "atr-87-#892",    "atr-88-#ca9",   "atr-89-#f29",    "atr-90-#885",
                  "atr-91-#25",    "atr-92-#ae9",    "atr-93-#bd9",    "atr-94-#1010",   "atr-95-#8e",    "atr-96-#235",    "atr-97-#125",
                  "atr-98-#23c",   "atr-99-#12c",    "atr-100-#429",   "atr-101-#1a9",   "atr-102-#1298", "atr-103-#3839",  "atr-104-#b71",
                  "atr-105-#2b",   "atr-106-#89b",   "atr-107-#b8f",   "atr-108-#10b5",  "atr-109-#838",  "atr-110-#8b0",   "atr-111-#bc4",
                  "atr-112-#20",   "atr-113-#b30",   "atr-114-#2ce",   "atr-115-#1be",   "atr-116-#db",   "atr-117-#1011",  "atr-118-#2b9",
                  "atr-119-#1c9",  "atr-120-#c78",   "atr-121-#d9",    "atr-122-#9a8",   "atr-123-#1b29", "atr-124-#12e",   "atr-125-#23e",
                  "atr-126-#28c",  "atr-127-#19c",   "atr-128-#bcb",   "atr-129-#8bf",   "atr-130-#f09",  "atr-131-#cc9",   "atr-132-#82d",
                  "atr-133-#b71",  "atr-134-#2c5",   "atr-135-#1b5",   "atr-136-#1a9",   "atr-137-#429",  "atr-138-#2929",  "atr-139-#1b18",
                  "atr-140-#19",   "atr-141-#108",   "atr-142-#1b01",  "atr-143-#1b15",  "atr-144-#24",   "atr-145-#b58",   "atr-146-#100a",
                  "atr-147-#8c8",  "atr-148-#811",   "atr-149-#b05",   "atr-150-#1b31",  "atr-151-#1b25", "atr-152-#3c",    "atr-153-#bb1",
                  "atr-154-#22d",  "atr-155-#13d",   "atr-156-#93",    "atr-157-#1a18",  "atr-158-#238",  "atr-159-#128",   "atr-160-#32",
                  "atr-161-#19d8", "atr-162-#b89",   "atr-163-#a99",   "atr-164-#9b",    "atr-165-#6db",  "atr-166-#105",   "atr-167-#d",
                  "atr-168-#5",    "atr-169-#348",   "atr-170-#202",   "atr-171-#112",   "atr-172-#85",   "atr-173-#2",     "atr-174-#8d8",
                  "atr-175-#183f", "atr-176-#a48",   "atr-177-#b58",   "atr-178-#9e9",   "atr-179-#1bdb", "atr-180-#378",   "atr-181-#25",
                  "atr-182-#123e", "atr-183-#122a",  "atr-184-#b1c",   "atr-185-#846",   "atr-186-#8c2",  "atr-187-#bb6",   "atr-188-#11e9",
                  "atr-189-#33",   "atr-190-#8db",   "atr-191-#b9d",   "atr-192-#29e9",  "atr-193-#84",   "atr-194-#1bcf",  "atr-195-#1bbb",
                  "atr-196-#20d",  "atr-197-#2d",    "atr-198-#202",   "atr-199-#112",   "atr-200-#5",    "atr-201-#e29",   "atr-202-#22",
                  "atr-203-#969",  "atr-204-#28f",   "atr-205-#19f",   "atr-206-#8b",    "atr-207-#271",  "atr-208-#246",   "atr-209-#156",
                  "atr-210-#8b",   "atr-211-#10b5",  "atr-212-#196",   "atr-213-#286",   "atr-214-#22",   "atr-215-#814",   "atr-216-#828",
                  "atr-217-#13a8", "atr-218-#9d",    "atr-219-#cb9",   "atr-220-#13b8",  "atr-221-#38a9", "atr-222-#6d8",   "atr-223-#378",
                  "atr-224-#8c6",  "atr-225-#bb2",   "atr-226-#b56",   "atr-227-#80c",   "atr-228-#939",  "atr-229-#28e8",  "atr-230-#34",
                  "atr-231-#83b",  "atr-232-#f39",   "atr-233-#e29",   "atr-234-#1235",  "atr-235-#1221", "atr-236-#28f",   "atr-237-#19f",
                  "atr-238-#22",   "atr-239-#190",   "atr-240-#6f8",   "atr-241-#358",   "atr-242-#11d8", "atr-243-#59d9",  "atr-244-#b1e",
                  "atr-245-#80a",  "atr-246-#8c4",   "atr-247-#bb0",   "atr-248-#9f9",   "atr-249-#c9",   "atr-250-#bc7",   "atr-251-#8b3",
                  "atr-252-#181c", "atr-253-#8c9",   "atr-254-#179",   "atr-255-#269",   "atr-256-#228",  "atr-257-#138",   "atr-258-#1a78",
                  "atr-259-#1868", "atr-260-#f28",   "atr-261-#ca8",   "atr-262-#18d8",  "atr-263-#978",  "atr-264-#1b2",   "atr-265-#f6",
                  "atr-266-#246",  "atr-267-#156",   "atr-268-#e8",    "atr-269-#1b3d",  "atr-270-#23f",  "atr-271-#12f",   "atr-272-#b5",
                  "atr-273-#1bb",  "atr-274-#823",   "atr-275-#b37",   "atr-276-#839",   "atr-277-#1b97", "atr-278-#f08",   "atr-279-#cc8",
                  "atr-280-#1c",   "atr-281-#459",   "atr-282-#1878",  "atr-283-#1a68",  "atr-284-#b27",  "atr-285-#833",   "atr-286-#885",
                  "atr-287-#b91",  "atr-288-#b58",   "atr-289-#a48",   "atr-290-#896",   "atr-291-#b82",  "atr-292-#a88",   "atr-293-#b98",
                  "atr-294-#39e8", "atr-295-#12f9",  "atr-296-#26",    "atr-297-#222",   "atr-298-#13d",  "atr-299-#22d",   "atr-300-#1b69",
                  "atr-301-#2958", "atr-302-#169",   "atr-303-#279",   "atr-304-#c6",    "atr-305-#5829", "atr-306-#b39",   "atr-307-#a29",
                  "atr-308-#8de",  "atr-309-#b9c",   "atr-310-#b08",   "atr-311-#a18",   "atr-312-#ba",   "atr-313-#b34",   "atr-314-#2ca",
                  "atr-315-#1ba",  "atr-316-#43",    "atr-317-#1728",  "atr-318-#18ca",  "atr-319-#18be", "atr-320-#a4",    "atr-321-#1068",
                  "atr-322-#cc8",  "atr-323-#f08",   "atr-324-#37",    "atr-325-#11a",   "atr-326-#1b4",  "atr-327-#2c4",   "atr-328-#8a",
                  "atr-329-#3f9",  "atr-330-#2b3",   "atr-331-#1c3",   "atr-332-#61",    "atr-333-#1b9",  "atr-334-#869",   "atr-335-#1005",
                  "atr-336-#af9",  "atr-337-#d59",   "atr-338-#978",   "atr-339-#1227",  "atr-340-#b68",  "atr-341-#a78",   "atr-342-#38d9",
                  "atr-343-#32",   "atr-344-#29c",   "atr-345-#18c",   "atr-346-#13e",   "atr-347-#22e",  "atr-348-#1041",  "atr-349-#101d",
                  "atr-350-#de",   "atr-351-#21d",   "atr-352-#1b8a",  "atr-353-#1b9e",  "atr-354-#4d",   "atr-355-#ac9",   "atr-356-#bc3",
                  "atr-357-#8b7",  "atr-358-#bce",   "atr-359-#8ba",   "atr-360-#2cf",   "atr-361-#1bf",  "atr-362-#113",   "atr-363-#203",
                  "atr-364-#a39",  "atr-365-#b29",   "atr-366-#1168",  "atr-367-#d",     "atr-368-#1b2a", "atr-369-#c38",   "atr-370-#b3c",
                  "atr-371-#866",  "atr-372-#b68",   "atr-373-#a78",   "atr-374-#1158",  "atr-375-#5959", "atr-376-#29c",   "atr-377-#18c",
                  "atr-378-#c9",   "atr-379-#185",   "atr-380-#876",   "atr-381-#b2c",   "atr-382-#b9e",  "atr-383-#88a",   "atr-384-#83c",
                  "atr-385-#1a",   "atr-386-#b9a",   "atr-387-#88e",   "atr-388-#f59",   "atr-389-#cf9",  "atr-390-#bb3",   "atr-391-#8c7",
                  "atr-392-#959",  "atr-393-#1bb3",  "atr-394-#d",     "atr-395-#639",   "atr-396-#241",  "atr-397-#b5",    "atr-398-#206",
                  "atr-399-#116",  "atr-400-#1851",  "atr-401-#9a8",   "atr-402-#fd8",   "atr-403-#c78",  "atr-404-#38",    "atr-405-#28c",
                  "atr-406-#23e",  "atr-407-#12e",   "atr-408-#329",   "atr-409-#6a9",   "atr-410-#13b",  "atr-411-#22b",   "atr-412-#169",
                  "atr-413-#279",  "atr-414-#1a38",  "atr-415-#8b9",   "atr-416-#b8f",   "atr-417-#89b",  "atr-418-#b80",   "atr-419-#894",
                  "atr-420-#1cf",  "atr-421-#2bf",   "atr-422-#213",   "atr-423-#103",   "atr-424-#9e9",  "atr-425-#1b17",  "atr-426-#c39",
                  "atr-427-#3818", "atr-428-#38",    "atr-429-#b0c",   "atr-430-#b98",   "atr-431-#a88",  "atr-432-#45",    "atr-433-#9a8",
                  "atr-434-#1289", "atr-435-#348",   "atr-436-#f6",    "atr-437-#28c",   "atr-438-#195",  "atr-439-#285",   "atr-440-#127",
                  "atr-441-#237",  "atr-442-#4b",    "atr-443-#191",   "atr-444-#bf9",   "atr-445-#58c8", "atr-446-#b0",    "atr-447-#18db",
                  "atr-448-#bb2",  "atr-449-#8c6",   "atr-450-#28",    "atr-451-#cf8",   "atr-452-#834",  "atr-453-#b20",   "atr-454-#2be",
                  "atr-455-#1ce",  "atr-456-#1d8",   "atr-457-#2e8",   "atr-458-#183f",  "atr-459-#182b", "atr-460-#c68",   "atr-461-#fe8",
                  "atr-462-#8a",   "atr-463-#1895",  "atr-464-#6a8",   "atr-465-#328",   "atr-466-#121e", "atr-467-#120a",  "atr-468-#1c7",
                  "atr-469-#2b7",  "atr-470-#21",    "atr-471-#18bf",  "atr-472-#113",   "atr-473-#203",  "atr-474-#b85",   "atr-475-#891",
                  "atr-476-#a",    "atr-477-#8d9",   "atr-478-#37",    "atr-479-#12c6",  "atr-480-#1349", "atr-481-#3878",  "atr-482-#459",
                  "atr-483-#c7",   "atr-484-#b91",   "atr-485-#885",   "atr-486-#833",   "atr-487-#14",   "atr-488-#ac8",   "atr-489-#bb8",
                  "atr-490-#b30",  "atr-491-#824",   "atr-492-#e58",   "atr-493-#f48",   "atr-494-#b2",   "atr-495-#12bc",  "atr-496-#608",
                  "atr-497-#3c8",  "atr-498-#1096",  "atr-499-#4c",    "atr-500-#287",   "atr-501-#197",  "atr-502-#121",   "atr-503-#90",
                  "atr-504-#1023", "atr-505-#829",   "atr-506-#ae9",   "atr-507-#bd9",   "atr-508-#b2a",  "atr-509-#83e",   "atr-510-#2e",
                  "atr-511-#d58",  "atr-512-#ad8",   "atr-513-#be8",   "atr-514-#18a9",  "atr-515-#2cb8", "atr-516-#206",   "atr-517-#116",
                  "atr-518-#241",  "atr-519-#81",    "atr-520-#b0c",   "atr-521-#856",   "atr-522-#ce",   "atr-523-#bc6",   "atr-524-#1bde",
                  "atr-525-#1b9c", "atr-526-#1f",    "atr-527-#178",   "atr-528-#183a",  "atr-529-#182e", "atr-530-#1f",    "atr-531-#195",
                  "atr-532-#120f", "atr-533-#121b",  "atr-534-#ce",    "atr-535-#b19",   "atr-536-#b25",  "atr-537-#831",   "atr-538-#41",
                  "atr-539-#876",  "atr-540-#8b0",   "atr-541-#1c",    "atr-542-#b0a",   "atr-543-#81e",  "atr-544-#b",     "atr-545-#91",
                  "atr-546-#1832", "atr-547-#1826",  "atr-548-#215",   "atr-549-#105",   "atr-550-#129f", "atr-551-#128b",  "atr-552-#2c1",
                  "atr-553-#8f",   "atr-554-#861",   "atr-555-#b3d",   "atr-556-#9b9",   "atr-557-#26",   "atr-558-#a38",   "atr-559-#b28",
                  "atr-560-#2a3",  "atr-561-#5",     "atr-562-#13f",   "atr-563-#22f",   "atr-564-#859",  "atr-565-#98",    "atr-566-#bc9",
                  "atr-567-#ab9",  "atr-568-#b36",   "atr-569-#822",   "atr-570-#968",   "atr-571-#1b66", "atr-572-#8b0",   "atr-573-#bc4",
                  "atr-574-#8",    "atr-575-#12a",   "atr-576-#28a8",  "atr-577-#39",    "atr-578-#3f9",  "atr-579-#659",   "atr-580-#f6",
                  "atr-581-#1208", "atr-582-#cb8",   "atr-583-#f18",   "atr-584-#166",   "atr-585-#276",  "atr-586-#28a",   "atr-587-#19a",
                  "atr-588-#ca",   "atr-589-#559",   "atr-590-#2c3",   "atr-591-#1b3",   "atr-592-#237",  "atr-593-#127",   "atr-594-#8bf",
                  "atr-595-#bcb",  "atr-596-#1af9",  "atr-597-#bf9",   "atr-598-#968",   "atr-599-#100f", "atr-600-#2cf",   "atr-601-#1bf",
                  "atr-602-#113",  "atr-603-#69",    "atr-604-#a39",   "atr-605-#b29",   "atr-606-#d",    "atr-607-#88",    "atr-608-#80c",
                  "atr-609-#b56",  "atr-610-#1881",  "atr-611-#76",    "atr-612-#b96",   "atr-613-#882",  "atr-614-#100",   "atr-615-#97",
                  "atr-616-#1c28", "atr-617-#2819",  "atr-618-#3c9",   "atr-619-#2a",    "atr-620-#2829", "atr-621-#1b88",  "atr-622-#c68",
                  "atr-623-#fe8",  "atr-624-#5958",  "atr-625-#1159",  "atr-626-#6a8",   "atr-627-#f1",   "atr-628-#21b",   "atr-629-#10b",
                  "atr-630-#199",  "atr-631-#289",   "atr-632-#2cf",   "atr-633-#1bf",   "atr-634-#1f",   "atr-635-#b33",   "atr-636-#a39",
                  "atr-637-#b29",  "atr-638-#9b8",   "atr-639-#69",    "atr-640-#13c",   "atr-641-#22c",  "atr-642-#9a",    "atr-643-#18e",
                  "atr-644-#10b3", "atr-645-#8b8",   "atr-646-#31",    "atr-647-#b48",   "atr-648-#891",  "atr-649-#b85",   "atr-650-#5899",
                  "atr-651-#24",   "atr-652-#b0d",   "atr-653-#851",   "atr-654-#19b",   "atr-655-#8d",   "atr-656-#115",   "atr-657-#205",
                  "atr-658-#27",   "atr-659-#20c",   "atr-660-#b2f",   "atr-661-#83b",   "atr-662-#88d",  "atr-663-#ba3",   "atr-664-#189a",
                  "atr-665-#188e", "atr-666-#609",   "atr-667-#c3",    "atr-668-#280",   "atr-669-#190",  "atr-670-#638",   "atr-671-#528",
                  "atr-672-#13c",  "atr-673-#22c",   "atr-674-#bb6",   "atr-675-#8c2",   "atr-676-#1bbe", "atr-677-#8b8",   "atr-678-#9a",
                  "atr-679-#fa9",  "atr-680-#268",   "atr-681-#178",   "atr-682-#1a59",  "atr-683-#1849", "atr-684-#8b2",   "atr-685-#bc6",
                  "atr-686-#18",   "atr-687-#856",   "atr-688-#861",   "atr-689-#b3d",   "atr-690-#18",   "atr-691-#1879",  "atr-692-#bcf",
                  "atr-693-#8bb",  "atr-694-#123",   "atr-695-#233",   "atr-696-#1b1e",  "atr-697-#1b0a", "atr-698-#248",   "atr-699-#158",
                  "atr-700-#1c0",  "atr-701-#2b0",   "atr-702-#21e",   "atr-703-#10e",   "atr-704-#b68",  "atr-705-#a78",   "atr-706-#1817",
                  "atr-707-#1803", "atr-708-#b35",   "atr-709-#821",   "atr-710-#cd",    "atr-711-#12b4", "atr-712-#b9b",   "atr-713-#88f",
                  "atr-714-#10d",  "atr-715-#21d",   "atr-716-#59a8",  "atr-717-#11a9",  "atr-718-#bb",   "atr-719-#208",   "atr-720-#bc3",
                  "atr-721-#8b7",  "atr-722-#83",    "atr-723-#b0f",   "atr-724-#1cf8",  "atr-725-#28e9", "atr-726-#2c",    "atr-727-#729",
                  "atr-728-#216",  "atr-729-#106",   "atr-730-#1b1f",  "atr-731-#3f",    "atr-732-#1de",  "atr-733-#2a4",   "atr-734-#b1a",
                  "atr-735-#96",   "atr-736-#b68",   "atr-737-#a78",   "atr-738-#d8",    "atr-739-#9f9",  "atr-740-#af9",   "atr-741-#99",
                  "atr-742-#13d9", "atr-743-#819",   "atr-744-#cb",    "atr-745-#201",   "atr-746-#2cd",  "atr-747-#1bd",   "atr-748-#102e",
                  "atr-749-#103a", "atr-750-#286",   "atr-751-#196",   "atr-752-#20a",   "atr-753-#c0",   "atr-754-#89c",   "atr-755-#bde",
                  "atr-756-#958",  "atr-757-#13",    "atr-758-#ad9",   "atr-759-#be9",   "atr-760-#59",   "atr-761-#298",   "atr-762-#29e9",
                  "atr-763-#1bf8", "atr-764-#b04",   "atr-765-#810",   "atr-766-#8be",   "atr-767-#bca",  "atr-768-#32",    "atr-769-#b02",
                  "atr-770-#c48",  "atr-771-#1809",  "atr-772-#93",    "atr-773-#8a4",   "atr-774-#11e",  "atr-775-#20e",   "atr-776-#278",
                  "atr-777-#168",  "atr-778-#3969",  "atr-779-#1258",  "atr-780-#26",    "atr-781-#c58",  "atr-782-#2858",  "atr-783-#9c8",
                  "atr-784-#288",  "atr-785-#198",   "atr-786-#5929",  "atr-787-#8",     "atr-788-#29d",  "atr-789-#25",    "atr-790-#2868",
                  "atr-791-#1c59", "atr-792-#233",   "atr-793-#123",   "atr-794-#8bb",   "atr-795-#bcf",  "atr-796-#18b6",  "atr-797-#18c2",
                  "atr-798-#fd8",  "atr-799-#c78",   "atr-800-#8c4",   "atr-801-#bb0",   "atr-802-#9c",   "atr-803-#80a",   "atr-804-#1cb8",
                  "atr-805-#28a9", "atr-806-#33",    "atr-807-#358",   "atr-808-#115",   "atr-809-#205",  "atr-810-#1b17",  "atr-811-#26",
                  "atr-812-#28d",  "atr-813-#19d",   "atr-814-#81f",   "atr-815-#8f",    "atr-816-#891",  "atr-817-#b85",   "atr-818-#25",
                  "atr-819-#c",    "atr-820-#2b5",   "atr-821-#1c5",   "atr-822-#a9",    "atr-823-#251",  "atr-824-#39c8",  "atr-825-#849",
                  "atr-826-#ac9",  "atr-827-#c1",    "atr-828-#b00",   "atr-829-#814",   "atr-830-#1014", "atr-831-#ba8",   "atr-832-#88c",
                  "atr-833-#4",    "atr-834-#21a",   "atr-835-#10a",   "atr-836-#3a8",   "atr-837-#628",  "atr-838-#b4",    "atr-839-#18b3",
                  "atr-840-#123a", "atr-841-#122e",  "atr-842-#119",   "atr-843-#209",   "atr-844-#837",  "atr-845-#b23",   "atr-846-#b95",
                  "atr-847-#881",  "atr-848-#8ce",   "atr-849-#bba",   "atr-850-#b78",   "atr-851-#a68",  "atr-852-#b2c",   "atr-853-#876",
                  "atr-854-#1c6",  "atr-855-#2b6",   "atr-856-#ce",    "atr-857-#189f",  "atr-858-#8e28", "atr-859-#1759",  "atr-860-#35",
                  "atr-861-#d58",  "atr-862-#1159",  "atr-863-#818",   "atr-864-#c4",    "atr-865-#114",  "atr-866-#1ba",   "atr-867-#2ca",
                  "atr-868-#259",  "atr-869-#149",   "atr-870-#193",   "atr-871-#283",   "atr-872-#bc",   "atr-873-#18cd",  "atr-874-#a29",
                  "atr-875-#b39",  "atr-876-#41",    "atr-877-#b23",   "atr-878-#83a",   "atr-879-#b2e",  "atr-880-#1b05",  "atr-881-#4a",
                  "atr-882-#6e9",  "atr-883-#369",   "atr-884-#88f",   "atr-885-#b3",    "atr-886-#b2d",  "atr-887-#871",   "atr-888-#c38",
                  "atr-889-#16",   "atr-890-#866",   "atr-891-#b3c",   "atr-892-#a78",   "atr-893-#cd",   "atr-894-#1b68",  "atr-895-#2959",
                  "atr-896-#878",  "atr-897-#1bc0",  "atr-898-#f49",   "atr-899-#81",    "atr-900-#b81",  "atr-901-#895",   "atr-902-#823",
                  "atr-903-#13",   "atr-904-#3f9",   "atr-905-#659",   "atr-906-#1b81",  "atr-907-#1b95", "atr-908-#266",   "atr-909-#176",
                  "atr-910-#b5",   "atr-911-#10b6",  "atr-912-#618",   "atr-913-#3b8",   "atr-914-#a8",   "atr-915-#978",   "atr-916-#bb4",
                  "atr-917-#8c0",  "atr-918-#bbb",   "atr-919-#b6",    "atr-920-#2ba",   "atr-921-#1ca",  "atr-922-#9b",    "atr-923-#214",
                  "atr-924-#1fa8", "atr-925-#1928",  "atr-926-#32",    "atr-927-#a98",   "atr-928-#81d",  "atr-929-#b41",   "atr-930-#be9",
                  "atr-931-#ad9",  "atr-932-#b81",   "atr-933-#895",   "atr-934-#117",   "atr-935-#207",  "atr-936-#3f9",   "atr-937-#659",
                  "atr-938-#a8",   "atr-939-#1000",  "atr-940-#6d8",   "atr-941-#378",   "atr-942-#1019", "atr-943-#5818",  "atr-944-#c38",
                  "atr-945-#c6",   "atr-946-#9e8",   "atr-947-#2969",  "atr-948-#bc7",   "atr-949-#8b3",  "atr-950-#9f9",   "atr-951-#5828",
                  "atr-952-#b13",  "atr-953-#ba",    "atr-954-#185",   "atr-955-#295",   "atr-956-#1284", "atr-957-#43",    "atr-958-#6f8",
                  "atr-959-#358",  "atr-960-#bc9",   "atr-961-#a4",    "atr-962-#859",   "atr-963-#1b18", "atr-964-#13f",   "atr-965-#37",
                  "atr-966-#2a3",  "atr-967-#1db",   "atr-968-#18b5",  "atr-969-#8a",    "atr-970-#1a4",  "atr-971-#2de",   "atr-972-#129a",
                  "atr-973-#61",   "atr-974-#e28",   "atr-975-#f38",   "atr-976-#1805",  "atr-977-#888",  "atr-978-#ba9",   "atr-979-#5968",
                  "atr-980-#db",   "atr-981-#10c4",  "atr-982-#2e8",   "atr-983-#1d8",   "atr-984-#882",  "atr-985-#b96",   "atr-986-#b20",
                  "atr-987-#834",  "atr-988-#9f",    "atr-989-#d29",   "atr-990-#b11",   "atr-991-#805",  "atr-992-#1e",    "atr-993-#a39",
                  "atr-994-#5928", "atr-995-#1129",  "atr-996-#cf",    "atr-997-#2cf",   "atr-998-#1b0",  "atr-999-#2c0",   "atr-1000-#b09",
                  "atr-1001-#a19", "atr-1002-#8f9",  "atr-1003-#1369", "atr-1004-#185",  "atr-1005-#295", "atr-1006-#223",  "atr-1007-#133",
                  "atr-1008-#104", "atr-1009-#214",  "atr-1010-#3868", "atr-1011-#1359", "atr-1012-#29e", "atr-1013-#18e",  "atr-1014-#846",
                  "atr-1015-#b1c", "atr-1016-#103a", "atr-1017-#a4",   "atr-1018-#b29",  "atr-1019-#a39", "atr-1020-#101e", "atr-1021-#1d",
                  "atr-1022-#458", "atr-1023-#1f8" });
} // namespace transactions
} // namespace couchbase

const std::string &couchbase::transactions::AtrIds::atr_id_for_vbucket(int vbucket_id)
{
    if (vbucket_id < 0 || vbucket_id > ATR_IDS.size()) {
        throw std::invalid_argument(std::string("invalid vbucket_id: ") + std::to_string(vbucket_id));
    }
    return ATR_IDS[vbucket_id];
}

#include "../deps/libcouchbase/src/vbucket/crc32.h"

int couchbase::transactions::AtrIds::vbucket_for_key(const std::string &key)
{
    static const int num_vbuckets = 1024;
    uint32_t digest = hash_crc32(key.data(), key.size());
    return digest % num_vbuckets;
}
