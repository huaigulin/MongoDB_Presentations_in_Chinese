// $match
db.movies.aggregate([
  {
    $match: {
      $and: [
        { "imdb.rating": { $gte: 7 } },
        { genres: { $nin: ["Crime", "Horror"] } },
        { rated: { $in: ["PG", "G"] } },
        { languages: { $all: ["English", "Japanese"] } },
      ],
    },
  },
]);

// $project: 比CRUD里的projection更有用，可以添加新的field
db.movies.aggregate([
  {
    $match: {
      $and: [
        { "imdb.rating": { $gte: 7 } },
        { genres: { $nin: ["Crime", "Horror"] } },
        { rated: { $in: ["PG", "G"] } },
        { languages: { $all: ["English", "Japanese"] } },
      ],
    },
  },
  {
    $project: {
      _id: 0,
      title: 1,
      tomato_rating: "$tomatoes.viewer.rating",
      imdb_rating: "$imdb.rating",
    },
  },
]);

// $addFields: 专门用来添加新的field
db.movies.aggregate([
  {
    $match: {
      $and: [
        { "imdb.rating": { $gte: 7 } },
        { genres: { $nin: ["Crime", "Horror"] } },
        { rated: { $in: ["PG", "G"] } },
        { languages: { $all: ["English", "Japanese"] } },
      ],
    },
  },
  {
    $addFields: {
      tomato_rating: "$tomatoes.viewer.rating",
      imdb_rating: "$imdb.rating",
    },
  },
]);

// $group: 根据你选择的field分组
db.movies.aggregate([
  {
    $group: {
      _id: "$year",
      count: { $sum: 1 },
    },
  },
  {
    $sort: { count: -1 },
  },
]);

// 根据一部电影的导演数量分组，但有些电影document没有directors array，所以需要检查
db.movies.aggregate([
  {
    $group: {
      _id: {
        numDirectors: {
          $cond: [{ $isArray: "$directors" }, { $size: "$directors" }, 0],
        },
      },
      numFilms: { $sum: 1 },
      averageMetacritic: { $avg: "$metacritic" },
    },
  },
  {
    $sort: { "_id.numDirectors": -1 },
  },
]);

// $unwind: 拆开documents里的array
db.movies.aggregate([
  {
    $match: {
      "imdb.rating": { $gt: 0 },
      year: { $gte: 2010, $lte: 2015 },
      runtime: { $gte: 90 },
    },
  },
  {
    $unwind: "$genres",
  },
  {
    $group: {
      _id: {
        year: "$year",
        genre: "$genres",
      },
      average_rating: { $avg: "$imdb.rating" },
    },
  },
  {
    $sort: { "_id.year": -1, average_rating: -1 },
  },
]);

// $lookup: 相当于SQL里的left join
db.air_alliances.aggregate([
  {
    $lookup: {
      from: "air_airlines",
      localField: "airlines",
      foreignField: "name",
      as: "airlines",
    },
  },
]);

// $bucket
db.movies.aggregate([
  {
    $bucket: {
      groupBy: "$imdb.rating",
      boundaries: [0, 5, 8, Infinity],
      default: "not rated",
      output: {
        average_per_bucket: { $avg: "$imdb.rating" },
        count: { $sum: 1 },
      },
    },
  },
]);

// $bucketAuto
db.movies.aggregate([
  {
    $bucketAuto: {
      groupBy: "$title",
      buckets: 4,
    },
  },
]);

// $facet
db.movies.aggregate([
  {
    $match: {
      metacritic: { $gte: 0 },
      "imdb.rating": { $gte: 0 },
    },
  },
  {
    $project: {
      _id: 0,
      metacritic: 1,
      imdb: 1,
      title: 1,
    },
  },
  {
    $facet: {
      top_metacritic: [
        {
          $sort: {
            metacritic: -1,
            title: 1,
          },
        },
        {
          $limit: 10,
        },
        {
          $project: {
            title: 1,
          },
        },
      ],
      top_imdb: [
        {
          $sort: {
            "imdb.rating": -1,
            title: 1,
          },
        },
        {
          $limit: 10,
        },
        {
          $project: {
            title: 1,
          },
        },
      ],
    },
  },
  {
    $project: {
      movies_in_both: {
        $setIntersection: ["$top_metacritic", "$top_imdb"],
      },
    },
  },
]);
