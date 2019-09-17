--
-- PostgreSQL database dump
-- Stores the initial, empty database template

-- Dumped from database version 10.10 (Ubuntu 10.10-0ubuntu0.18.04.1)
-- Dumped by pg_dump version 10.10 (Ubuntu 10.10-0ubuntu0.18.04.1)

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

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: image_bbox; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.image_bbox (
    imageid character varying(30) NOT NULL,
    source character varying(12) NOT NULL,
    label character varying(20) NOT NULL,
    confidence character varying(10),
    x_min real,
    x_max real,
    y_min real,
    y_max real,
    is_occ character(1),
    is_tru character(1),
    is_grp character(1),
    is_dep character(1),
    is_ins character(1)
);


ALTER TABLE public.image_bbox OWNER TO postgres;

--
-- Name: image_labels; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.image_labels (
    imageid character varying(30) NOT NULL,
    source character varying(30) NOT NULL,
    label character varying(20) NOT NULL,
    confidence character varying(10)
);


ALTER TABLE public.image_labels OWNER TO postgres;

--
-- Name: image_selection; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.image_selection (
    imageid character varying(30) NOT NULL,
    obj_count integer NOT NULL,
    is_manual character(1) NOT NULL,
    image_labels character varying(500)
);


ALTER TABLE public.image_selection OWNER TO postgres;

--
-- Name: label_names; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.label_names (
    label character varying(20) NOT NULL,
    name character varying(100) NOT NULL
);


ALTER TABLE public.label_names OWNER TO postgres;

--
-- Data for Name: image_bbox; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.image_bbox (imageid, source, label, confidence, x_min, x_max, y_min, y_max, is_occ, is_tru, is_grp, is_dep, is_ins) FROM stdin;
\.


--
-- Data for Name: image_labels; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.image_labels (imageid, source, label, confidence) FROM stdin;
\.


--
-- Data for Name: image_selection; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.image_selection (imageid, obj_count, is_manual, image_labels) FROM stdin;
\.


--
-- Data for Name: label_names; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.label_names (label, name) FROM stdin;
\.


--
-- Name: image_bbox image_bbox_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.image_bbox
    ADD CONSTRAINT image_bbox_pkey PRIMARY KEY (imageid);


--
-- Name: image_labels image_labels_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.image_labels
    ADD CONSTRAINT image_labels_pkey PRIMARY KEY (imageid);


--
-- Name: image_selection image_selection_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.image_selection
    ADD CONSTRAINT image_selection_pkey PRIMARY KEY (imageid);


--
-- Name: label_names label_names_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.label_names
    ADD CONSTRAINT label_names_pkey PRIMARY KEY (label);


--
-- PostgreSQL database dump complete
--

