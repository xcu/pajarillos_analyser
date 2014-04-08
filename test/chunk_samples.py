from datetime import datetime

chunk_container_sample = {
    "_id" : "5249997039d092f9c75b3dbf",
    "chunks" : {},
    "chunk_size" : 100,
    "current_chunk" : "52499970e138235994c416a3",
    "start_date" : datetime(2013, 8, 16, 9, 48),
    "size" : 10
}

chunk_container_sample_bad_size = {
    "_id" : "5249997039d092f9c75b3dbf",
    "chunks" : {},
    "chunk_size" : 100,
    "current_chunk" : "52499970e138235994c416a3",
    "start_date" : datetime(2013, 8, 16, 9, 48),
    "size" : 100
}

chunk_sample = {
    "parent_container": datetime(2013, 8, 16, 9, 48),
    "_id" : "524997c6e138235893e1bdf6",
    "terms" : {
        "de" : 25,
        "y" : 14,
        "http" : 14,
        "co" : 14,
        "es" : 14,
        "por" : 6,
        "s" : 6,
        "o" : 6,
        "n" : 6,
    },
    "sorted_terms" : [
        [-25, ["de"]],
        [-14, ["y", "http", "co", "es"]],
        [-6, ["s", "por", "o", "n"]],
    ],
    "user_mentions" : {
        "javierbarrado" : 1,
        "Fulendstambulen" : 4,
        "Los40_Spain" : 2,
        "williamlevybra" : 1,
    },
    "sorted_user_mentions" : [
        [-4, ["Fulendstambulen"]],
        [-2, ["Los40_Spain"]],
        [-1, ["javierbarrado", "williamlevybra"]]
    ],
    "hashtags" : {
        "PutaVidaTete" : 1,
        "10CosasQueOdio" : 1,
        "nature" : 3
    },
    "sorted_hashtags" : [
        [-3, ["nature"]],
        [-1, ["PutaVidaTete", "10CosasQueOdio"]]
    ],
    "users" : [
        "DCRLogistica",
        "LuciaMs10",
        "TiniMyWorldLife",
        "AmaliaGarciaaaa",
        "Evscup",
        "AMMenaBernal99",
        "paulovich_96"
    ],
    "tweet_ids" : [
        "384700378669125632",
        "384700365578719233",
        "384700297148641280",
        "384700275787038721",
        "384700341109145600",
        "384700268698697729",
        "384700323228831744",
        "384700355596255232",
        "384700299958824960",
        "384700317621035008",
    ]
}

chunk_sample_small1 = {"parent_container": datetime(2013, 8, 16, 9, 49),
                      "terms": {'froyo': 8, 'pollo': 20},
                      "sorted_terms" : [[-20, ['pollo']], [-8, ['froyo']]],
                      "user_mentions" : {"williamlevybra": 1},
                      "sorted_user_mentions" : [[-1, ["williamlevybra"]]],
                      "hashtags" : {"10CosasQueOdio" : 2},
                      "sorted_hashtags" : [[-2, ["10CosasQueOdio"]]],
                      "users": ["chiquito", "de_la_calzada"],
                      "tweet_ids": ["584700299958824960"]}

chunk_sample_small2 = {"parent_container": datetime(2013, 8, 16, 9, 50),
                      "terms": {'froyo': 7, 'chollo': 10},
                      "sorted_terms" : [[-10, ['chollo']], [-7, ['froyo']]],
                      "user_mentions" : {"el_fary": 4},
                      "sorted_user_mentions" : [[-4, ["el_fary"]]],
                      "hashtags" : {"10CosasQueOdio" : 1},
                      "sorted_hashtags" : [[-1, ["10CosasQueOdio"]]],
                      "users": ["corcho", "con", "tiopaco"],
                      "tweet_ids": ["58470023599958824960"]}

chunk_container_with_chunks = {
    "_id" : "5249997039d092f9c75b3dcf",
    "chunks" : {"1": chunk_sample_small1.copy(),
                "2": chunk_sample_small2.copy()},
    "chunk_size" : 100,
    "current_chunk" : "52499970e138235994c416a3",
    "start_date" : datetime(2013, 8, 16, 9, 48),
    "size" : 10
}
