'''
Cleanup for top_3_out
'''

import csv
import re

d = {'Apps_for_Android': {'buy_after_viewing': [], 'also_viewed': [(b'Apps for Android', 117112), (b'Games', 45885), (b'Entertainment', 9427)], 'also_bought': [(b'Apps for Android', 3480670), (b'Games', 2707995), (b'Kindle Store', 159351)]}, 'Tools_and_Home_Improvement': {'buy_after_viewing': [(b'Tools & Home Improvement', 368468), (b'Power & Hand Tools', 159439), (b'Hand Tools', 63520)], 'also_viewed': [(b'Tools & Home Improvement', 1854634), (b'Power & Hand Tools', 618781), (b'Lighting & Ceiling Fans', 381879)], 'also_bought': [(b'Tools & Home Improvement', 1504472), (b'Power & Hand Tools', 881393), (b'Hand Tools', 339510)]}, 'Patio,_Lawn_and_Garden': {'buy_after_viewing': [(b'Patio, Lawn & Garden', 4580), (b'Gardening & Lawn Care', 1713), (b'Tools & Home Improvement', 1254)], 'also_viewed': [(b'Patio, Lawn & Garden', 1990128), (b'Gardening & Lawn Care', 811423), (b'Plants, Seeds & Bulbs', 274258)], 'also_bought': [(b'Patio, Lawn & Garden', 962805), (b'Gardening & Lawn Care', 508023), (b'Plants, Seeds & Bulbs', 241579)]}, 'Video_Games': {'buy_after_viewing': [(b'Video Games', 182454), (b'Games', 102128), (b'PC', 34724)], 'also_viewed': [(b'Video Games', 245537), (b'Games', 105662), (b'Accessories', 89333)], 'also_bought': [(b'Video Games', 1890848), (b'Games', 1179168), (b'More Systems', 333238)]}, 'Baby': {'buy_after_viewing': [(b'Baby', 116826), (b'Toys & Games', 8606), (b'Baby Products', 7720)], 'also_viewed': [(b'Baby', 1191697), (b'Toys & Games', 79632), (b'Baby Products', 52189)], 'also_bought': [(b'Baby', 905943), (b'Baby Products', 137614), (b'Clothing, Shoes & Jewelry', 76293)]}, 'Digital_Music': {'buy_after_viewing': [(b'Digital Music', 148900), (b'CDs & Vinyl', 85193), (b'Pop', 25369)], 'also_viewed': [(b'CDs & Vinyl', 16522), (b'Digital Music', 15012), (b'Pop', 6356)], 'also_bought': [(b'Digital Music', 5204935), (b'CDs & Vinyl', 1204869), (b'Pop', 357043)]}, 'Automotive': {'buy_after_viewing': [(b'Automotive', 10347), (b'Tools & Equipment', 4368), (b'Tools & Home Improvement', 3206)], 'also_viewed': [(b'Automotive', 3662130), (b'Replacement Parts', 778119), (b'Exterior Accessories', 592300)], 'also_bought': [(b'Automotive', 1599104), (b'Replacement Parts', 371298), (b'Exterior Accessories', 234884)]}, 'Home_and_Kitchen': {'buy_after_viewing': [(b'Home & Kitchen', 11468), (b'Tools & Home Improvement', 4508), (b'Kitchen & Dining', 3747)], 'also_viewed': [(b'Home & Kitchen', 5790067), (b'Kitchen & Dining', 3291380), (b'Bedding', 751432)], 'also_bought': [(b'Home & Kitchen', 1756248), (b'Kitchen & Dining', 1209119), (b'Kitchen Utensils & Gadgets', 285980)]}, 'Musical_Instruments': {'buy_after_viewing': [(b'Musical Instruments', 125469), (b'Instrument Accessories', 66511), (b'CDs & Vinyl', 54893)], 'also_viewed': [(b'Musical Instruments', 504218), (b'Instrument Accessories', 115275), (b'Guitars', 79473)], 'also_bought': [(b'Musical Instruments', 584350), (b'CDs & Vinyl', 467020), (b'Instrument Accessories', 360270)]}, 'Office_Products': {'buy_after_viewing': [(b'Office Products', 61764), (b'Office & School Supplies', 42114), (b'Printer Ink & Toner', 35568)], 'also_viewed': [(b'Office Products', 1369783), (b'Office & School Supplies', 1136337), (b'Writing & Correction Supplies', 278746)], 'also_bought': [(b'Office Products', 901130), (b'Office & School Supplies', 849109), (b'Writing & Correction Supplies', 187260)]}, 'Pet_Supplies': {'buy_after_viewing': [(b'Pet Supplies', 388), (b'Dogs', 192), (b'Books', 79)], 'also_viewed': [(b'Pet Supplies', 1707887), (b'Dogs', 1000463), (b'Cats', 246851)], 'also_bought': [(b'Pet Supplies', 1246474), (b'Dogs', 611616), (b'Toys', 248375)]}, 'Grocery_and_Gourmet_Food': {'buy_after_viewing': [(b'Grocery & Gourmet Food', 55), (b'CDs & Vinyl', 15), (b'Beverages', 8)], 'also_viewed': [(b'Grocery & Gourmet Food', 1932785), (b'Health & Personal Care', 98003), (b'Home & Kitchen', 86659)], 'also_bought': [(b'Grocery & Gourmet Food', 1619661), (b'Health & Personal Care', 141939), (b'Home & Kitchen', 95593)]}, 'Clothing,_Shoes_and_Jewelry': {'buy_after_viewing': [(b'Clothing, Shoes & Jewelry', 3170), (b'Novelty, Costumes & More', 968), (b'Men', 875)], 'also_viewed': [(b'Clothing, Shoes & Jewelry', 26227085), (b'Women', 7978425), (b'Men', 3897906)], 'also_bought': [(b'Clothing, Shoes & Jewelry', 13559092), (b'Women', 5016742), (b'Clothing', 3498567)]}, 'Sports_and_Outdoors': {'buy_after_viewing': [(b'Sports & Outdoors', 8191), (b'Hunting & Fishing', 3517), (b'Tools & Home Improvement', 2917)], 'also_viewed': [(b'Sports & Outdoors', 4559065), (b'Clothing, Shoes & Jewelry', 1678184), (b'Hunting & Fishing', 1403056)], 'also_bought': [(b'Sports & Outdoors', 2721240), (b'Clothing, Shoes & Jewelry', 935749), (b'Hunting & Fishing', 884978)]}, 'Movies_and_TV': {'buy_after_viewing': [(b'Movies & TV', 244751), (b'Movies', 126315), (b'TV', 117047)], 'also_viewed': [(b'Movies & TV', 186471), (b'Movies', 116894), (b'TV', 68590)], 'also_bought': [(b'Movies & TV', 3136178), (b'Movies', 1809331), (b'TV', 1309800)]}, 'Electronics': {'buy_after_viewing': [(b'Electronics', 856296), (b'Computers & Accessories', 390365), (b'Camera & Photo', 173433)], 'also_viewed': [(b'Electronics', 3229804), (b'Computers & Accessories', 1350514), (b'Camera & Photo', 658731)], 'also_bought': [(b'Electronics', 2990378), (b'Computers & Accessories', 1321796), (b'Camera & Photo', 766172)]}, 'Cell_Phones_and_Accessories': {'buy_after_viewing': [(b'Cell Phones & Accessories', 663143), (b'Cases', 337772), (b'Basic Cases', 300929)], 'also_viewed': [(b'Cell Phones & Accessories', 1411846), (b'Cases', 944138), (b'Basic Cases', 785750)], 'also_bought': [(b'Cell Phones & Accessories', 2356369), (b'Cases', 1397534), (b'Basic Cases', 1259283)]}, 'CDs_and_Vinyl': {'buy_after_viewing': [(b'CDs & Vinyl', 2098234), (b'Pop', 554436), (b'Rock', 280087)], 'also_viewed': [(b'CDs & Vinyl', 1768805), (b'Pop', 464817), (b'Rock', 223921)], 'also_bought': [(b'CDs & Vinyl', 18483629), (b'Pop', 5157988), (b'Rock', 2945361)]}, 'Health_and_Personal_Care': {'buy_after_viewing': [(b'Health & Personal Care', 9027), (b'Household Supplies', 6299), (b'Household Batteries', 4594)], 'also_viewed': [(b'Health & Personal Care', 3392138), (b'Vitamins & Dietary Supplements', 824956), (b'Health Care', 598017)], 'also_bought': [(b'Health & Personal Care', 2254482), (b'Vitamins & Dietary Supplements', 762023), (b'Health Care', 400828)]}, 'Amazon_Instant_Video': {'buy_after_viewing': [], 'also_viewed': [(b'Video Games', 352), (b'Kitchen & Dining', 191), (b'Home & Kitchen', 191)], 'also_bought': []}, 'Toys_and_Games': {'buy_after_viewing': [(b'Toys & Games', 3867), (b'Musical Instruments', 1618), (b'Video Games', 992)], 'also_viewed': [(b'Toys & Games', 6344641), (b'Action Figures & Statues', 1208119), (b'Games', 961327)], 'also_bought': [(b'Toys & Games', 5330517), (b'Games', 1110998), (b'Action Figures & Statues', 984690)]}, 'Beauty': {'buy_after_viewing': [(b'CDs & Vinyl', 2049), (b'Pop', 504), (b'Beauty', 338)], 'also_viewed': [(b'Beauty', 3134328), (b'Hair Care', 906478), (b'Skin Care', 886802)], 'also_bought': [(b'Beauty', 2020581), (b'Skin Care', 598861), (b'Makeup', 542748)]}, 'Books': {'buy_after_viewing': [(b'Books', 3460545), (b'Kindle Store', 1283745), (b'Kindle eBooks', 1118842)], 'also_viewed': [(b'Books', 2097612), (b'Kindle Store', 1229007), (b'Kindle eBooks', 1032309)], 'also_bought': [(b'Books', 48249362), (b'Kindle Store', 26983557), (b'Kindle eBooks', 23643741)]}, 'Kindle_Store': {'buy_after_viewing': [(b'Kindle Store', 1299085), (b'Kindle eBooks', 1127759), (b'Books', 1123558)], 'also_viewed': [(b'Kindle Store', 1243418), (b'Kindle eBooks', 1040301), (b'Books', 1033023)], 'also_bought': [(b'Kindle Store', 27276074), (b'Kindle eBooks', 23818434), (b'Books', 23580724)]}}

def main():
	top_d = {}
	main = []
	with open('top_3_out.csv', 'w') as csvfile:
		writer = csv.writer(csvfile)
		writer.writerow(["Category","","Also_viewed","","","","","","","Also_bought","","","","","","","Buy_after_viewing"])
		for c in d.items():
			cat = c[0].replace("_", " ")
			cat = cat.replace("and", "&")
			also_viewed = re.findall(r"b'([\w &,]+)', (\d+)", str(c[1]['also_viewed']))
			viewed_out = []
			for item in also_viewed:
				viewed_out.append(item[0])
				viewed_out.append(item[1])

				if item[0] in top_d.keys():
					top_d[item[0]] += int(item[1])
				else:
					top_d[item[0]] = int(item[1])
			while len(viewed_out) < 6:
				viewed_out.append("")

			also_bought = re.findall(r"b'([\w &,]+)', (\d+)", str(c[1]['also_bought']))
			bought_out = []
			for item in also_bought:
				bought_out.append(item[0])
				bought_out.append(item[1])

				if item[0] in top_d.keys():
					top_d[item[0]] += int(item[1])
				else:
					top_d[item[0]] = int(item[1])
			while len(bought_out) < 6:
				bought_out.append("")
			buy_after_viewing = re.findall(r"b'([\w &,]+)', (\d+)", str(c[1]['buy_after_viewing']))
			buy_out = []
			for item in buy_after_viewing:
				buy_out.append(item[0])
				buy_out.append(item[1])

				if item[0] in top_d.keys():
					top_d[item[0]] += int(item[1])
				else:
					top_d[item[0]] = int(item[1])
			while len(buy_out) < 6:
				buy_out.append("")
			writer.writerow([cat] +[""]+ viewed_out+[""]+ bought_out+[""]+ buy_out)

	return sorted(top_d.items(), key = lambda x: x[1], reverse = True)[:10]

if __name__ == '__main__':
	d = main()
	for key, value in d:
		print(key, value)