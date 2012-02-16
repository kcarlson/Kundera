/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client.junit;

import javax.persistence.EntityManager;

import org.apache.log4j.Logger;

/**
 * Test case for CRUD operations on Cassandra database using Kundera.
 */
public class TestCassandra extends BaseTest
{

    /** The manager. */
    private EntityManager manager;

    /** The logger. */
    private static Logger logger = Logger.getLogger(TestCassandra.class);

    /**
     * The embedded server cassandra.
     *
     * @throws Exception the exception
     */
    // private static EmbeddedCassandraService cassandra;

    public void startCassandraServer() throws Exception
    {
        super.startCassandraServer();
    }

    /**
     * Sets the up.
     *
     * @throws Exception the exception
     */
    public void setUp() throws Exception
    {
        logger.info("starting server");
        /*
         * if (cassandra == null) { startCassandraServer(); }
         */
        // manager =
        // Persistence.createEntityManagerFactory("cassandra").createEntityManager();
        // startCassandraServer();

    }

    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    public void tearDown() throws Exception
    {
        logger.info("destroying");
    }

    /*
     * public void testInsertUser() { User user = new User();
     * user.setUserId("IIIPL-0001");
     * 
     * PersonalDetail pd = new PersonalDetail(); pd.setPersonalDetailId("1");
     * pd.setName("Amresh"); pd.setPassword("password1");
     * pd.setRelationshipStatus("single"); user.setPersonalDetail(pd);
     * 
     * user.addTweet(new Tweet("a", "Here it goes, my first tweet", "web"));
     * user.addTweet(new Tweet("b", "Another one from me", "mobile"));
     * 
     * user.setPreference(new Preference("1", "Serene", "5"));
     * 
     * user.addImDetail(new IMDetail("1", "Yahoo", "xamry"));
     * user.addImDetail(new IMDetail("2", "GTalk", "amry_4u"));
     * user.addImDetail(new IMDetail("3", "MSN", "itsmeamry"));
     * 
     * manager.persist(user); }
     * 
     * 
     * public void testUpdateUser() { User user = manager.find(User.class,
     * "IIIPL-0001");
     * 
     * PersonalDetail pd = new PersonalDetail();
     * 
     * pd.setPassword("password2"); pd.setRelationshipStatus("married");
     * 
     * List<Tweet> tweets = user.getTweets();
     * 
     * for(Tweet tweet : tweets) { if(tweet.getTweetId().equals("a")) {
     * tweet.setBody("My first tweet is now modified"); } else
     * if(tweet.getTweetId().equals("b")) {
     * tweet.setBody("My second tweet is now modified"); } }
     * 
     * tweets.add(new Tweet("c", "My Third tweet", "iPhone"));
     * 
     * manager.persist(user); }
     * 
     * 
     * 
     * public void testFindUser() { User user = manager.find(User.class,
     * "IIIPL-0001"); System.out.println(user.getUserId() + "(Personal Data): "
     * + user.getPersonalDetail().getName() + "/Tweets:" + user.getTweets()); }
     * 
     * 
     * 
     * public void testDeleteUser() { User user = manager.find(User.class,
     * "IIIPL-0001"); System.out.println(user); manager.remove(user); }
     *//**
       * Test save authors.
       *
       */
    /*
     * 
     * public void testSaveAuthors() throws Exception {
     * logger.info("onTestSaveAuthors"); String key = System.currentTimeMillis()
     * + "-author"; Author animesh = createAuthor(key, "animesh@animesh.org",
     * "India", new Date()); manager.persist(animesh);
     * 
     * // check if saved? Author animesh_db = manager.find(Author.class, key);
     * assertEquals(animesh, animesh_db); }
     *//**
       * Test save posts.
       * 
       * @throws Exception
       *             the exception
       */
    /*
     * public void testSavePosts() throws Exception {
     * logger.info("onTestSavePosts"); String key = System.currentTimeMillis() +
     * "-post"; Thread.sleep(5000); Post post = createPost(key,
     * "I hate love stories", "I hate - Imran Khan, Sonal Kapoor", "Animesh",
     * new Date(), "movies", "hindi"); manager.persist(post);
     * 
     * String sql = "Select p.body from Post p where p.title like :tiitle";
     * Query query = manager.createQuery(sql);
     * 
     * query.setParameter("tiitle", "love"); List<Post> posts =
     * query.getResultList(); assertTrue(!posts.isEmpty());
     * 
     * for (Post p : posts) { assertNotNull(p.getData().getCreated());
     * assertNull(p.getAuthor().getAuthor());
     * assertNull(p.getAuthor().getEmail());
     * assertNotNull(p.getData().getTitle());
     * assertNotNull(p.getData().getBody()); }
     * 
     * // check if saved? Post post_db = manager.find(Post.class, key);
     * assertEquals(post, post_db); }
     *//**
       * _test delete authors.
       * 
       * @throws Exception
       *             the exception
       */
    /*
     * 
     * public void testDeleteAuthors() throws Exception {
     * logger.info("ontestDeleteAuthors");
     * 
     * String key = System.currentTimeMillis() + "-animesh";
     * 
     * // save new author Author animesh = createAuthor(key,
     * "animesh@animesh.org", "India", new Date()); manager.persist(animesh);
     * 
     * // delete this author manager.remove(animesh);
     * 
     * // check if deleted? Author animesh_db = manager.find(Author.class, key);
     * assertEquals(null, animesh_db); }
     *//**
       * Creates the author.
       * 
       * @param username
       *            the user name
       * @param email
       *            the email
       * @param country
       *            the country
       * @param registeredSince
       *            the registered since
       * 
       * @return the author
       */
    /*
     * 
     * private static Author createAuthor(String username, String email, String
     * country, Date registeredSince) { Author author = new Author();
     * author.setUsername(username); author.setCountry(country);
     * author.setEmailAddress(email); author.setRegistered(registeredSince);
     * return author; }
     *//**
       * Creates the post.
       * 
       * @param permalink
       *            the permalink
       * @param title
       *            the title
       * @param body
       *            the body
       * @param author
       *            the author
       * @param created
       *            the created
       * @param tags
       *            the tags
       * 
       * @return the post
       */
    /*
     * private static Post createPost(String permalink, String title, String
     * body, String author, Date created, String... tags) { Post post = new
     * Post(); AuthorDetail authorDetail = new AuthorDetail(); PostData data =
     * new PostData(); data.setTitle(title); post.setPermalink(permalink);
     * data.setBody(body); authorDetail.setAuthor(author);
     * authorDetail.setEmail("impetus@impetus.com"); data.setCreated(created);
     * post.setAuthor(authorDetail); post.setData(data); //
     * post.setTags(Arrays.asList(tags)); return post; }
     */
    public void testSnsUser()
    {
        // for (int i = 0; i <= 1; i++)
        // {
        // SnsUser user = new SnsUser();
        // user.setSnstype("snstype" + i);
        // user.setSnsuid(i + "");
        // user.setBlessuid(i + "");
        // user.setLocalId(i + "");
        // user.setAccountName("account" + i);
        // manager.persist(user);
        // SnsUser found = manager.find(SnsUser.class, i + "");
        // assertNotNull(found);
        // assertNotNull(found.getAccountName());
        // }
        // for
    }

    /*
     * public void testQuery() { Query q =
     * manager.createQuery("select u from User u"); //q.setParameter("subject",
     * "Join"); //q.setParameter("body", "Please Join Meeting"); List<User>
     * users = q.getResultList(); System.out.println("Users:" + users); }
     */

}