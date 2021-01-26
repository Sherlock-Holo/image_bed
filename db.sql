--
-- PostgreSQL database dump
--

-- Dumped from database version 13.1
-- Dumped by pg_dump version 13.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: id_generate; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.id_generate
(
    id_type  text   NOT NULL,
    id_value bigint NOT NULL
);


ALTER TABLE public.id_generate
    OWNER TO postgres;

--
-- Name: resources; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.resources
(
    id            text   NOT NULL,
    bucket        text   NOT NULL,
    create_time   bigint NOT NULL,
    hash          text   NOT NULL,
    resource_size bigint NOT NULL
);


ALTER TABLE public.resources
    OWNER TO postgres;

--
-- Name: COLUMN resources.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.resources.id IS 'resource id';


--
-- Name: COLUMN resources.bucket; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.resources.bucket IS 'resource bucket';


--
-- Name: COLUMN resources.create_time; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.resources.create_time IS 'resouce create time';


--
-- Name: COLUMN resources.hash; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.resources.hash IS 'resource hash';


--
-- Data for Name: id_generate; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.id_generate (id_type, id_value) FROM stdin;
image_bed	3
test_id	21
\.


--
-- Data for Name: resources; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.resources (id, bucket, create_time, hash, resource_size) FROM stdin;
\.


--
-- Name: id_generate id_generate_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.id_generate
    ADD CONSTRAINT id_generate_pk PRIMARY KEY (id_type);


--
-- Name: resources resources_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_pk PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

